export interface Env {
	DB: D1Database;
}

function decodeDailyRawData(buffer: ArrayBuffer): number[] {
	if (buffer.byteLength !== 96) {
		throw new Error("Invalid RawData length");
	}

	const view = new DataView(buffer);
	const hours: number[] = [];

	for (let i = 0; i < 24; i++) {
		hours.push(view.getUint32(i * 4, true));
	}

	return hours;
}

export default {
	async fetch(request: Request, env: Env): Promise<Response> {
		const url = new URL(request.url);
		if (request.method === "POST" && url.pathname === "/insert-history") {
			const body = await request.json<{
				guid: number[];
				datetime: number;
				rawData: string;
			}>();

			const raw = Uint8Array.from(
				atob(body.rawData),
				c => c.charCodeAt(0)
			);

			await env.DB
				.prepare(
					`INSERT OR REPLACE INTO historyData
					 (Guid, Datetime, RawData)
					 VALUES (?, ?, ?)`
				)
				.bind(new Uint8Array(body.guid), body.datetime, raw)
				.run();

			await env.DB
				.prepare(
					`INSERT OR IGNORE INTO device (Guid, Name)
					 VALUES (?, ?)`
				)
				.bind(new Uint8Array(body.guid), null)
				.run();

			return new Response(
				JSON.stringify({ ok: true }),
				{ headers: { "content-type": "application/json" } }
			);
		}

		if (request.method === "POST" && url.pathname === "/query-history") {
			const body = await request.json<{
				guid: number[];
				datetime: number;
			}>();

			const row = await env.DB
				.prepare(
					`SELECT RawData
					 FROM historyData
					 WHERE Guid = ? AND Datetime = ?`
				)
				.bind(new Uint8Array(body.guid), body.datetime)
				.first<{
					RawData: ArrayBuffer;
				}>();

			if (!row) {
				return new Response(
					JSON.stringify({ hours: [] }),
					{ status: 404, headers: { "content-type": "application/json" } }
				);
			}

			const hours = decodeDailyRawData(row.RawData);

			return new Response(
				JSON.stringify({
					datetime: body.datetime,
					hours
				}),
				{ headers: { "content-type": "application/json" } }
			);
		}

		return new Response("Not Found", { status: 404 });
	}
} satisfies ExportedHandler<Env>;
