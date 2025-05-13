use aruna_metadata::models::requests::{
    AddUserRequest, AddUserResponse, CreateResourceRequest, CreateResourceResponse, SearchRequest,
};
use criterion::{Criterion, criterion_group, criterion_main};
//use persistors::{TantivyFjall, TantivyHeed, TantivyRedb};
use persistors::{TantivyFjall, TantivyHeed};
use rand::seq::IteratorRandom;
use std::time::Duration;

pub mod persistors;

fn e2e_benchmark(c: &mut Criterion) {
    let variant = dotenvy::var("VARIANT").unwrap();
    //let port = dotenvy::var("API_PORT").unwrap();

    // Isolated runtime for tantivy/heed
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let (client, base_url, user1, user2) = rt.block_on(async {
        let client = reqwest::Client::new();
        let base_url = format!("http://localhost:8080/api/v3");

        let request = AddUserRequest {
            name: "bench_user1".to_string(),
        };
        let response: AddUserResponse = client
            .post(format!("{base_url}/users"))
            .json(&request)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let user1 = response.user.id;
        println!("{}", user1.to_string());

        let request = AddUserRequest {
            name: "bench_user2".to_string(),
        };

        let response: AddUserResponse = client
            .post(format!("{base_url}/users"))
            .json(&request)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let user2 = response.user.id;
        println!("{}", user2.to_string());

        (client, base_url, user1, user2)
    });

    c.bench_function(format!("benches/e2e/create/{variant}").as_ref(), |b| {
        b.to_async(&rt).iter(|| async {
            let base_urls = [
                format!("http://localhost:8080/api/v3/resources"),
                format!("http://localhost:8081/api/v3/resources"),
                format!("http://localhost:8082/api/v3/resources"),
                format!("http://localhost:8083/api/v3/resources"),
            ];
            //for i in 0..10_000 {
            for i in 0..100 {
                let create_resource = CreateResourceRequest {
                    name: format!("res{i}"),
                    ..Default::default()
                };
                //if i < 4999 {
                if i < 49 {
                    let _response: CreateResourceResponse = client
                        .post(base_urls.iter().choose(&mut rand::rngs::OsRng).unwrap())
                        .header::<&str, &str>(
                            "Authorization",
                            format!("Bearer {}", user1.to_string()).as_ref(),
                        )
                        .json(&create_resource)
                        .send()
                        .await
                        .unwrap()
                        .json()
                        .await
                        .unwrap();
                } else {
                    let _response: CreateResourceResponse = client
                        .post(base_urls.iter().choose(&mut rand::rngs::OsRng).unwrap())
                        .header::<&str, &str>(
                            "Authorization",
                            format!("Bearer {}", user2.to_string()).as_ref(),
                        )
                        .json(&create_resource)
                        .send()
                        .await
                        .unwrap()
                        .json()
                        .await
                        .unwrap();
                }
            }
        })
    });
}

fn controller_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("aruna_benches");

    // Isolated runtime for tantivy/fjall
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let (controller, user1, user2) = rt.block_on(async {
        let controller = TantivyFjall::start().await;
        let (user1, user2) = TantivyFjall::create_user(controller.clone()).await;
        (controller, user1, user2)
    });
    group
        .sample_size(100)
        .bench_function("tantivy/fjall:create", |b| {
            // Insert a call to `to_async` to convert the bencher to async mode.
            // The timing loops are the same as with the normal bencher.
            b.to_async(&rt).iter(|| async {
                TantivyFjall::bench_create(controller.clone(), user1, user2).await;
            })
        });

    group
        .sample_size(100)
        .bench_function("tantivy/fjall:search", |b| {
            // Insert a call to `to_async` to convert the bencher to async mode.
            // The timing loops are the same as with the normal bencher.
            b.to_async(&rt).iter(|| async {
                TantivyFjall::bench_search(controller.clone(), user1, user2).await;
            })
        });

    rt.shutdown_timeout(Duration::from_secs(60));
    //
    //     // Isolated runtime for tantivy/redb
    //     let rt = tokio::runtime::Builder::new_multi_thread()
    //         .enable_all()
    //         .build()
    //         .unwrap();
    //     let (controller, user1, user2) = rt.block_on(async {
    //         let controller = TantivyRedb::start().await;
    //         let (user1, user2) = TantivyRedb::create_user(controller.clone()).await;
    //         (controller, user1, user2)
    //     });
    //     group
    //         .sample_size(100)
    //         .bench_function("tantivy/redb:create", |b| {
    //             // Insert a call to `to_async` to convert the bencher to async mode.
    //             // The timing loops are the same as with the normal bencher.
    //             b.to_async(&rt).iter(|| async {
    //                 TantivyRedb::bench_create(controller.clone(), user1, user2).await;
    //             });
    //             //b.to_async(&rt).iter_custom(|_iter| {
    //             //    let clone = controller.clone();
    //             //    async move {
    //             //        let (user1, user2) = TantivyRedb::create_user(clone.clone()).await;
    //             //        let start = Instant::now();
    //             //        TantivyRedb::bench_create(clone.clone(), user1, user2).await;
    //             //        let elapsed = start.elapsed();
    //
    //             //        clone.clear().await.unwrap();
    //             //        elapsed
    //             //    }
    //             //});
    //         });
    //
    //     group
    //         .sample_size(100)
    //         .bench_function("tantivy/redb:search", |b| {
    //             // Insert a call to `to_async` to convert the bencher to async mode.
    //             // The timing loops are the same as with the normal bencher.
    //             b.to_async(&rt).iter(|| async {
    //                 TantivyRedb::bench_search(controller.clone(), user1, user2).await;
    //             });
    //             //b.to_async(&rt).iter_custom(|_iter| {
    //             //    let clone = controller.clone();
    //             //    async move {
    //             //        let (user1, user2) = TantivyRedb::create_user(clone.clone()).await;
    //             //        TantivyRedb::bench_create(clone.clone(), user1, user2).await;
    //             //        let start = Instant::now();
    //             //        TantivyRedb::bench_search(clone.clone(), user1, user2).await;
    //             //        let elapsed = start.elapsed();
    //
    //             //        clone.clear().await.unwrap();
    //             //        elapsed
    //             //    }
    //             //});
    //         });
    //
    //     rt.shutdown_timeout(Duration::from_secs(60));

    // Isolated runtime for tantivy/heed
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let (controller, user1, user2) = rt.block_on(async {
        let controller = TantivyHeed::start().await;
        let (user1, user2) = TantivyHeed::create_user(controller.clone()).await;
        (controller, user1, user2)
    });
    group
        .sample_size(100)
        .bench_function("tantivy/heed:create", |b| {
            // Insert a call to `to_async` to convert the bencher to async mode.
            // The timing loops are the same as with the normal bencher.
            b.to_async(&rt).iter(|| async {
                TantivyHeed::bench_create(controller.clone(), user1, user2).await;
            });
        });

    group
        .sample_size(100)
        .bench_function("tantivy/heed:search", |b| {
            // Insert a call to `to_async` to convert the bencher to async mode.
            // The timing loops are the same as with the normal bencher.
            b.to_async(&rt).iter(|| async {
                TantivyHeed::bench_search(controller.clone(), user1, user2).await;
            });
        });

    group.finish();
}

//criterion_group!(benches, controller_benchmark);
criterion_group!(benches, e2e_benchmark);
criterion_main!(benches);
