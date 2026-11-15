// Types of the JavaScript library for Aruna plugins, for editors and type checks.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

export type Outcome = "permanent" | "retryable" | "invalid-input";

/** Every error the library throws; uncaught, `outcome` says how the run ends. */
export declare class ArunaError extends Error {
  outcome: Outcome;
  constructor(message: string);
}
export declare class NotAllowed extends ArunaError {}
export declare class NotFound extends ArunaError {}
export declare class Conflict extends ArunaError {}
export declare class LimitReached extends ArunaError {}
export declare class InvalidCall extends ArunaError {}
export declare class Unavailable extends ArunaError {}
export declare class UnknownOutcome extends ArunaError {}
export declare class ServiceError extends ArunaError {
  status: number;
  constructor(status: number, message: string);
}
export declare class Fail extends ArunaError {}
export declare class Retry extends ArunaError {}
export declare class InvalidInput extends ArunaError {}

/** The `[manual]` fields a person gave, defaults filled in; `dataset` and `object` fields loaded. */
export type Form = { [field: string]: any };
type Handler<T> = ((subject: T) => unknown) | (() => unknown);

export declare function on(trigger: "manual", handler: Handler<Form>): typeof handler;
export declare function on(trigger: "timer", handler: Handler<Date>): typeof handler;
export declare function on(trigger: "webhook", handler: Handler<Request>): typeof handler;
export declare function on(trigger: "dataset_created" | "dataset_updated", handler: Handler<Dataset>): typeof handler;
export declare function on(trigger: "data_uploaded", handler: Handler<BucketObject>): typeof handler;
export declare function on(trigger: "sync_completed" | "sync_failed", handler: Handler<SyncEvent>): typeof handler;
export declare function on(trigger: "job_finished", handler: Handler<Job>): typeof handler;
export declare function on(trigger: string | string[], handler: Handler<any>): typeof handler;

export interface WatchEvent {
  kind: string;
  path: string;
  resource: string;
  revision: string;
}

/** Who runs the handler, and the raw event of an event trigger. */
export declare const run: { id: string; install: string; group: string; event: WatchEvent | null };
/** The install's settings by name. */
export declare const settings: { [name: string]: any };

export declare const log: {
  debug(message: unknown): void;
  info(message: unknown): void;
  warn(message: unknown): void;
  error(message: unknown): void;
};

/** A verified webhook request; authentication headers are already removed. */
export declare class Request {
  method: string;
  query: string;
  headers: { [name: string]: string };
  body: Uint8Array;
  deliveryId: string | null;
  readonly text: string;
  json(): any;
}

/** A sync relationship's job that completed or failed, below the source path. */
export declare class SyncEvent {
  ok: boolean;
  path: string;
  relationship: string;
  job: string;
}

/** One dataset at the revision it was read; other properties by name with `get` and `set`. */
export declare class Dataset {
  id: string;
  revision: string;
  name: string;
  description: string;
  keywords: string[];
  license: string | null;
  identifiers: { [scheme: string]: any };
  get(name: string): any;
  set(name: string, value: unknown): void;
  remove(name: string): void;
  /** Sends what changed and answers the changed names; a change made meanwhile throws `Conflict`. */
  save(): string[];
  reload(): void;
  crate(): any;
  files(): Iterable<any>;
  addFile(object: BucketObject | string, properties?: { id?: string; [name: string]: unknown }): void;
  putFile(file: object): void;
  removeFile(id: string): void;
}

/** The JSON merge patch that turns `old` into `now`, with `null` for each name removed. */
export declare function mergePatch(old: object, now: object): { [name: string]: unknown };

export declare const datasets: {
  get(id: string): Dataset;
  list(): Iterable<Dataset>;
  create(path: string, fields: { [name: string]: unknown }): Dataset;
};

/** One object version; a `marked` one was written by a job with a secret and cannot be read. */
declare class BucketObject {
  path: string;
  size: number;
  version: string;
  marked: boolean;
  readonly url: string;
  read(): Uint8Array;
  readText(): string;
  readJson(): any;
}
export { BucketObject as Object };

export declare const objects: {
  get(path: string): BucketObject;
  list(prefix: string): Iterable<BucketObject>;
  copy(source: string, target: string, options?: { version?: string }): Job;
};

/** A service's whole answer. */
export declare class Reply {
  status: number;
  headers: { [name: string]: string };
  body: Uint8Array;
  readonly text: string;
  json(): any;
}

export interface RequestOptions {
  json?: unknown;
  body?: string | Uint8Array;
  query?: { [name: string]: unknown };
  headers?: { [name: string]: unknown };
  /** A status outside 200 to 299 throws `ServiceError` unless this is false. */
  check?: boolean;
}

export declare class Service {
  name: string;
  request(method: string, path?: string, options?: RequestOptions): Reply;
  get(path?: string | RequestOptions, options?: RequestOptions): Reply;
  head(path?: string | RequestOptions, options?: RequestOptions): Reply;
  post(path?: string | RequestOptions, options?: RequestOptions): Reply;
  put(path?: string | RequestOptions, options?: RequestOptions): Reply;
  patch(path?: string | RequestOptions, options?: RequestOptions): Reply;
  delete(path?: string | RequestOptions, options?: RequestOptions): Reply;
}

export declare function service(name: string): Service;
export declare function secret(name: string): { [field: string]: string };

export declare const state: {
  get<T = any>(key: string, fallback?: T): T;
  set(key: string, value: unknown): void;
  update<T = any>(key: string, change: (value: T) => T, fallback?: T): T;
  remove(key: string): void;
};

/** One job of the install; `state` is queued, running, succeeded, failed, error, cancelled or
 * unknown. */
export declare class Job {
  id: string;
  kind: string;
  path: string;
  state: string;
  readonly ok: boolean;
  message: string | null;
  bytes: number | null;
  version: string | null;
  exitCode: number | null;
  reply: Reply | null;
  report: string | null;
  refresh(): void;
  cancel(): void;
  outputs(): Iterable<BucketObject>;
  reportJson(): any;
}

export declare const jobs: {
  get(id: string): Job;
  copy(source: string, target: string, options?: { version?: string }): Job;
};

/** Puts a host in place of the component's imports; for the build's entry and tests. */
export declare function use(host: object, types?: { [field: string]: string }): void;

declare const aruna: {
  on: typeof on;
  run: typeof run;
  settings: typeof settings;
  log: typeof log;
  datasets: typeof datasets;
  objects: typeof objects;
  jobs: typeof jobs;
  service: typeof service;
  secret: typeof secret;
  state: typeof state;
  Dataset: typeof Dataset;
  Job: typeof Job;
  Object: typeof BucketObject;
  Reply: typeof Reply;
  Request: typeof Request;
  Service: typeof Service;
  SyncEvent: typeof SyncEvent;
  ArunaError: typeof ArunaError;
  Conflict: typeof Conflict;
  Fail: typeof Fail;
  InvalidCall: typeof InvalidCall;
  InvalidInput: typeof InvalidInput;
  LimitReached: typeof LimitReached;
  NotAllowed: typeof NotAllowed;
  NotFound: typeof NotFound;
  Retry: typeof Retry;
  ServiceError: typeof ServiceError;
  Unavailable: typeof Unavailable;
  UnknownOutcome: typeof UnknownOutcome;
};
export default aruna;
