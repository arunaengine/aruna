const MAIN_SOURCE: &str = include_str!("../src/main.rs");
const TASK_SOURCE: &str = include_str!("../../operations/src/task_incoming.rs");

#[test]
fn startup_ownership() {
    let recovery = MAIN_SOURCE
        .find(".recover_stale_jobs(")
        .expect("main must recover stale jobs");
    let runtime = MAIN_SOURCE
        .find("jobs_runtime.start();")
        .expect("main must start the jobs runtime");
    let queues = MAIN_SOURCE
        .find("task_queues.start(&shutdown).await;")
        .expect("main must start task queues");
    let timer = MAIN_SOURCE
        .find("restore_job_queue_timer(")
        .expect("main must restore the job timer");

    assert_eq!(MAIN_SOURCE.matches(".recover_stale_jobs(").count(), 1);
    assert_eq!(MAIN_SOURCE.matches("restore_job_queue_timer(").count(), 1);
    assert!(recovery < runtime && runtime < queues && queues < timer);

    let task_prod = TASK_SOURCE
        .split_once("\n#[cfg(test)]\nmod tests")
        .map(|(source, _)| source)
        .expect("task production boundary must exist");
    assert_eq!(task_prod.matches(".recover_stale_jobs(").count(), 0);
    assert_eq!(task_prod.matches("restore_job_queue_timer(").count(), 1);

    let queues_start = task_prod
        .find("impl TaskQueues {")
        .expect("TaskQueues implementation must exist");
    let queues_end = task_prod[queues_start..]
        .find("/// Kicks the installed document-sync drain owner")
        .map(|offset| queues_start + offset)
        .expect("TaskQueues implementation boundary must exist");
    let queues_source = &task_prod[queues_start..queues_end];
    assert!(!queues_source.contains(".recover_stale_jobs("));
    assert!(!queues_source.contains("restore_job_queue_timer("));

    let rearm_start = task_prod
        .find("async fn durable_rearm_loop(")
        .expect("durable rearm loop must exist");
    let rearm_end = task_prod[rearm_start..]
        .find("\n}\n\n// A batch that processed nothing")
        .map(|offset| rearm_start + offset)
        .expect("durable rearm loop boundary must exist");
    let rearm_source = &task_prod[rearm_start..rearm_end];
    assert_eq!(rearm_source.matches("restore_job_queue_timer(").count(), 1);
}
