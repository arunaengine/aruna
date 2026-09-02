// Fresh builds overflow the default query depth in nested async layouts.
#![recursion_limit = "256"]
mod shared;

use reqwest::{Response, StatusCode};
use serde_json::{Value, json};
use shared::{TestResult, create_bearer_token, create_group_via_http, spawn_seed_node};

async fn response_json(response: Response, expected: StatusCode) -> TestResult<Value> {
    let status = response.status();
    let body = response.text().await?;
    assert_eq!(status, expected, "unexpected response: {body}");
    Ok(serde_json::from_str(&body)?)
}

#[tokio::test(flavor = "multi_thread")]
async fn admin_job_flow() -> TestResult<()> {
    let seed = spawn_seed_node().await?;
    let result = async {
        let bearer = create_bearer_token(
            seed.context.as_ref(),
            seed.user_id,
            seed.realm_id,
            seed.capabilities.clone(),
        )
        .await?;
        let group = create_group_via_http(&seed.base_url, &bearer, "job-api-flow").await?;
        let client = reqwest::Client::new();

        let config_url = format!("{}/api/v1/compute/config", seed.base_url);
        let mut config = response_json(
            client.get(&config_url).bearer_auth(&bearer).send().await?,
            StatusCode::OK,
        )
        .await?;
        config["witness_base_delay_ms"] = json!(1_000);
        config["default_group_quota"]["max_jobs"] = json!(4);
        config["group_quotas"] = json!([{
            "group_id": group.group_id,
            "quota": {"max_jobs": 2}
        }]);
        let stored = response_json(
            client
                .put(&config_url)
                .bearer_auth(&bearer)
                .json(&config)
                .send()
                .await?,
            StatusCode::OK,
        )
        .await?;
        assert_eq!(stored["witness_base_delay_ms"], 1_000);
        assert_eq!(stored["default_group_quota"]["max_jobs"], 4);

        let snapshots_url = format!(
            "{}/api/v1/compute/snapshots?group_id={}",
            seed.base_url, group.group_id
        );
        let snapshots = response_json(
            client
                .get(&snapshots_url)
                .bearer_auth(&bearer)
                .send()
                .await?,
            StatusCode::OK,
        )
        .await?;
        assert_eq!(snapshots["group"]["group_id"], group.group_id);
        assert_eq!(snapshots["group"]["quota"]["max_jobs"], 2);

        let invalid_snapshots = client
            .get(format!(
                "{}/api/v1/compute/snapshots?group_id=invalid",
                seed.base_url
            ))
            .bearer_auth(&bearer)
            .send()
            .await?;
        assert_eq!(invalid_snapshots.status(), StatusCode::BAD_REQUEST);

        let drain_url = format!("{}/api/v1/compute/drain", seed.base_url);
        let drained = response_json(
            client
                .post(&drain_url)
                .bearer_auth(&bearer)
                .json(&json!({"draining": true}))
                .send()
                .await?,
            StatusCode::OK,
        )
        .await?;
        assert_eq!(drained["draining"], true);
        assert_eq!(drained["changed"], true);
        let unchanged = response_json(
            client
                .post(&drain_url)
                .bearer_auth(&bearer)
                .json(&json!({"draining": true}))
                .send()
                .await?,
            StatusCode::OK,
        )
        .await?;
        assert_eq!(unchanged["changed"], false);

        let request = json!({
            "group_id": group.group_id,
            "image": "busybox:latest",
            "command": ["true"],
            "cpu_cores": 1,
            "ram_bytes": 1_048_576,
            "idempotency_key": "single-node-job-api-flow",
            "workspace": {"mode": "none"}
        });
        let jobs_url = format!("{}/api/v1/compute/jobs", seed.base_url);
        let submitted = response_json(
            client
                .post(&jobs_url)
                .bearer_auth(&bearer)
                .json(&request)
                .send()
                .await?,
            StatusCode::CREATED,
        )
        .await?;
        assert_eq!(submitted["created"], true);
        assert_eq!(submitted["state"], "queued");
        let job_id = submitted["job_id"]
            .as_str()
            .ok_or_else(|| std::io::Error::other("submission has no job id"))?;

        let replayed = response_json(
            client
                .post(&jobs_url)
                .bearer_auth(&bearer)
                .json(&request)
                .send()
                .await?,
            StatusCode::OK,
        )
        .await?;
        assert_eq!(replayed["created"], false);
        assert_eq!(replayed["job_id"], job_id);
        assert_eq!(replayed["submission_id"], submitted["submission_id"]);

        let listed = response_json(
            client
                .get(format!("{jobs_url}?limit=1"))
                .bearer_auth(&bearer)
                .send()
                .await?,
            StatusCode::OK,
        )
        .await?;
        assert!(
            listed["jobs"]
                .as_array()
                .is_some_and(|jobs| jobs.iter().any(|job| job["job_id"] == job_id))
        );
        let invalid_list = client
            .get(format!("{jobs_url}?state=unknown"))
            .bearer_auth(&bearer)
            .send()
            .await?;
        assert_eq!(invalid_list.status(), StatusCode::BAD_REQUEST);

        let status_url = format!("{}/api/v1/compute/jobs/{job_id}", seed.base_url);
        let status = response_json(
            client.get(&status_url).bearer_auth(&bearer).send().await?,
            StatusCode::OK,
        )
        .await?;
        assert_eq!(status["job_id"], job_id);
        assert_eq!(
            status["family"]["submission_id"],
            submitted["submission_id"]
        );

        let audit_url = format!("{status_url}/audit");
        let audit = response_json(
            client.get(&audit_url).bearer_auth(&bearer).send().await?,
            StatusCode::OK,
        )
        .await?;
        let records = audit["records"]
            .as_array()
            .ok_or_else(|| std::io::Error::other("audit has no records"))?;
        assert!(records.iter().any(|record| record["kind"] == "spec"));
        assert!(records.iter().any(|record| record["kind"] == "claim"));
        assert!(
            records
                .iter()
                .all(|record| record.get("signature").is_none())
        );

        let invalid_audit = client
            .get(format!("{audit_url}?scope=unknown"))
            .bearer_auth(&bearer)
            .send()
            .await?;
        assert_eq!(invalid_audit.status(), StatusCode::BAD_REQUEST);
        let report = client
            .get(format!("{status_url}/report"))
            .bearer_auth(&bearer)
            .send()
            .await?;
        assert_eq!(report.status(), StatusCode::NOT_FOUND);
        let artifact = client
            .get(format!("{status_url}/artifact"))
            .bearer_auth(&bearer)
            .send()
            .await?;
        assert_eq!(artifact.status(), StatusCode::NOT_FOUND);

        let cancelled = client
            .post(format!("{status_url}/cancel"))
            .bearer_auth(&bearer)
            .send()
            .await?;
        assert!(matches!(
            cancelled.status(),
            StatusCode::OK | StatusCode::ACCEPTED
        ));
        let cancelled: Value = cancelled.json().await?;
        assert_eq!(cancelled["cancel_requested"], true);

        let cancelled_status = response_json(
            client.get(&status_url).bearer_auth(&bearer).send().await?,
            StatusCode::OK,
        )
        .await?;
        assert_eq!(cancelled_status["family"]["cancel_requested"], true);
        let cancelled_audit = response_json(
            client.get(&audit_url).bearer_auth(&bearer).send().await?,
            StatusCode::OK,
        )
        .await?;
        assert!(
            cancelled_audit["records"]
                .as_array()
                .is_some_and(|records| records.iter().any(|record| record["kind"] == "cancel"))
        );

        let undrained = response_json(
            client
                .post(&drain_url)
                .bearer_auth(&bearer)
                .json(&json!({"draining": false}))
                .send()
                .await?,
            StatusCode::OK,
        )
        .await?;
        assert_eq!(undrained["draining"], false);
        assert_eq!(undrained["changed"], true);

        Ok(())
    }
    .await;

    seed.shutdown().await;
    result
}
