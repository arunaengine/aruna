/**
 * Talks to Aruna from an applet page. Every call goes to `/_aruna/` on the applet's own origin;
 * the gateway adds the visitor's session, so the page never handles a token.
 */
export class ArunaError extends Error {
  constructor(
    readonly status: number,
    message: string,
  ) {
    super(message);
  }
}

/** Sends one request to an Aruna runtime route such as `session` or `runs/<id>/results`. */
export async function arunaFetch(route: string, init: RequestInit = {}): Promise<Response> {
  const response = await fetch(`/_aruna/${route}`, { credentials: "same-origin", ...init });
  if (!response.ok) throw new ArunaError(response.status, await response.text());
  return response;
}

/** Sends `body` as JSON, or nothing, and reads the JSON answer. */
export async function arunaJson<T>(route: string, method = "GET", body?: unknown): Promise<T> {
  const init: RequestInit = { method, headers: { Accept: "application/json" } };
  if (body !== undefined) {
    init.body = JSON.stringify(body);
    init.headers = { ...init.headers, "Content-Type": "application/json" };
  }
  return (await arunaFetch(route, init)).json() as Promise<T>;
}

/** Sends the visitor to sign in and back to `returnTo` on this applet. */
export function signIn(returnTo = location.pathname + location.search): void {
  location.assign(`/_aruna/auth/start?return=${encodeURIComponent(returnTo)}`);
}

export function signOut(): void {
  location.assign("/_aruna/auth/logout");
}
