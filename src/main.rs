use simple_logger::SimpleLogger;

extern crate aruna_server;

#[tokio::main]
async fn main() {
    // Initialize simple logger
    SimpleLogger::new()
        .with_level(log::LevelFilter::Info)
        .env()
        .init()
        .unwrap();

    let server = aruna_server::server::grpc_server::ServiceServer {};
    server.run().await;
}
