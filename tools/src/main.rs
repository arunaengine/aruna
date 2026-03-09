use aruna_tools::test_token::create_token;

#[tokio::main]
pub async fn main() {
    let token = create_token().await;
    println!("TOKEN: {token}");
}
