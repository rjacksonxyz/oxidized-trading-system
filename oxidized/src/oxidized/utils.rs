use polars::prelude::*;
use rayon::prelude::*;
use scraper::{Html, Selector};
use std::{error::Error, collections::HashMap};
use std::sync::Mutex;
use yahoo_finance::{history, Bar};

pub async fn get_tickers_info() -> Result<DataFrame, Box<dyn Error>> {
    //submit a simple HTTP request
    let response = ureq::get("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies").call();
    let response_str = response.unwrap().into_string()?;

    let document = Html::parse_document(response_str.as_str());
    let table_selector = Selector::parse("table").unwrap();
    let mut columns = vec![];
    let mut values = vec![];

    // Iterate over html table
    if let Some(table) = document.select(&table_selector).next() {
        for (i, row) in table.select(&Selector::parse("tr").unwrap()).enumerate() {
            let cells: Vec<String> = row
                .select(&Selector::parse("td, th").unwrap())
                .filter_map(|cell| extract_link_text(cell.inner_html()))
                .collect();
            if i == 0 {
                columns = cells;
            } else {
                values.push(cells);
            }
        }
    }

    // Construct Vec<Series> need for Polars dataframe (has to be constructed column by column)
    let series: Vec<Series> = (0..values[0].len())
        .into_par_iter()
        .map(|step| {
            let column_values: Vec<String> = values.iter().map(|row| row[step].clone()).collect();
            Series::new(&columns[step], column_values)
        })
        .collect();

    // Create new DataFrame
    let df = DataFrame::new(series).unwrap();
    Ok(df)
}

fn extract_link_text(input: String) -> Option<String> {
    if input.contains("<a") {
        let start = input.find('>').map(|pos| pos + 1)?;
        let end = input[start..].find('<').map(|pos| pos + start)?;
        Some(input[start..end].to_string())
    } else {
        Some(input)
    }
}

/*
   internally, yahoo_finance returns data in the form of Vec<Bar>
   Bar {
       timestamp: 1674484200000,
       open: 59.29999923706055,
       high: 60.209999084472656,
       low: 58.9900016784668,
       close: 60.209999084472656,
       volume: Some(
           1072300,
       ),
   },
*/
// TODO: parallelize the process of Vec<Bar> data, obscenely slow otherwise.
// NOTE: Attempted to execute `retrieve_interval` requests with Rayon, works for CPU bound,
// non-trivial with async, need to understand more about async/await,
// need to reassess approach. For now, will leave as a single loop (very slow)
pub async fn get_sp500_df() -> Result<HashMap<String , DataFrame>, Box<dyn Error>> {
    let mut interval_data: HashMap<String, DataFrame> = HashMap::new();
    let df = get_tickers_info().await?;
    let symbol_series = df.column("Symbol").unwrap().utf8()?;
    let limit = 25; // artificial limit to avoid rate limiting from yahoo finance API
    let mut step = 0;
    for symbol in symbol_series.into_iter() {
        // Create Mutex-protected Vectors for each field since we'll be 
        let timestamps = Arc::new(Mutex::new(Vec::new()));
        let opens = Arc::new(Mutex::new(Vec::new()));
        let highs = Arc::new(Mutex::new(Vec::new()));
        let lows = Arc::new(Mutex::new(Vec::new()));
        let closes = Arc::new(Mutex::new(Vec::new()));
        let volumes = Arc::new(Mutex::new(Vec::new()));

        if step < limit {
            if let Some(sym) = symbol {
                let data = history::retrieve_interval(sym, yahoo_finance::Interval::_10y).await?;
                // once data is retrieved, convert into a dataframe and store in hash map
                // again, using rayon's parallel iterator to process structs into Vecs that will seed our Series
                data.par_iter().for_each(|bar| {
                    timestamps.lock().unwrap().push(bar.timestamp);
                    opens.lock().unwrap().push(bar.open);
                    highs.lock().unwrap().push(bar.high);
                    lows.lock().unwrap().push(bar.low);
                    closes.lock().unwrap().push(bar.close);
                    volumes.lock().unwrap().push(bar.volume.unwrap_or(0));
                });

                let timestamp_series = Series::new("timestamp", timestamps.lock().unwrap().as_slice());
                let open_series = Series::new("open", opens.lock().unwrap().as_slice());
                let high_series = Series::new("high", highs.lock().unwrap().as_slice());
                let low_series = Series::new("low", lows.lock().unwrap().as_slice());
                let close_series = Series::new("close", closes.lock().unwrap().as_slice());
                let volume_series = Series::new("volume", volumes.lock().unwrap().as_slice());

                let mut df = DataFrame::new(vec![
                    timestamp_series,
                    open_series,
                    high_series,
                    low_series,
                    close_series,
                    volume_series,
                ]).unwrap();
                //NOTE: Is it faster to force in-order appending or sorting? (intiution says sorting)
                df = df.sort(["timestamp"], false).unwrap();
                interval_data.insert(sym.to_string(),df);
                step += 1;
            }
        }
    }
    Ok(interval_data)
}
