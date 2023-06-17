use polars::prelude::*;
use scraper::{Html, Selector};
use std::error::Error;
use yahoo_finance::history;

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
    let mut series: Vec<Series> = Vec::new();
    let row_len = values[0].len();
    let mut step = 0;
    while step < row_len {
        let column_values: Vec<String> = values.iter().map(|row| row[step].clone()).collect();
        let col_series = Series::new(&columns[step], column_values);
        series.push(col_series);
        step += 1;
    }

    // Create new DataFrame
    let df = DataFrame::new_no_checks(series);
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
pub async fn get_sp500_df() -> Result<DataFrame, Box<dyn Error>> {
    let df = get_tickers_info().await?;
    let symbol_series = df.column("Symbol").unwrap().utf8()?;
    for symbol in symbol_series.into_iter() {
        if let Some(sym) = symbol {
            println!("getting data for {}", sym);
            let data = history::retrieve_interval(sym, yahoo_finance::Interval::_10y).await?;
            dbg!(data);
        }
    }
    Ok(df)
}