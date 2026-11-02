import { authHeaders } from "../auth";

export function apiFetch(input: RequestInfo | URL, init: RequestInit = {}): Promise<Response> {
  return fetch(input, {
    ...init,
    headers: { ...authHeaders(), ...init.headers },
  });
}

export function apiWebSocket(url: string): WebSocket {
  const token = authHeaders().Authorization?.replace(/^Bearer /, "") || "";
  return new WebSocket(url, ["kcl", "token." + token]);
}
