use polars::prelude::*;
use reqwest::Client;
use scraper::{Html, Selector};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create an async client
    let client = Client::builder().build()?;
    let response = client
        .get("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")
        .send()
        .await?
        .text()
        .await?;

    let document = Html::parse_document(&response);
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
    let mut row_step = 0;
    for col in columns {
        while row_step < row_len {
            let column_values: Vec<String> =
                values.iter().map(|row| row[row_step].clone()).collect();
            let col_series = Series::new(&col, column_values);
            series.push(col_series);
            row_step += 1;
        }
    }

    // Create new DataFrame
    let df = DataFrame::new_no_checks(series);

    // Sanity check
    println!("{:?}", df);

    Ok(())
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
