use oxidized::oxidized::utils::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let df = get_sp500_df().await;
        match df {
        Ok(info) => {
            println!("{:?}", info);
            return Ok(());
        }
        Err(e) => return Err(e),
    }
}