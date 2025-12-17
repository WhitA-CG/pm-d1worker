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

function base64ToUint8Array(b64: string): Uint8Array {
  const bin = atob(b64);
  const arr = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) {
    arr[i] = bin.charCodeAt(i);
  }
  return arr;
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    try {
      const url = new URL(request.url);

      // ===============================
      // POST /insert-history
      // ===============================
      if (request.method === "POST" && url.pathname === "/insert-history") {
        const body = await request.json();

        if (!Array.isArray(body.guid) || body.guid.length !== 16) {
          return new Response("Invalid guid", { status: 400 });
        }

        if (typeof body.datetime !== "number") {
          return new Response("Invalid datetime", { status: 400 });
        }

        if (typeof body.rawData !== "string") {
          return new Response("Invalid rawData", { status: 400 });
        }

        let raw: Uint8Array;
        try {
          raw = base64ToUint8Array(body.rawData);
        } catch {
          return new Response("Invalid base64 rawData", { status: 400 });
        }

        if (raw.byteLength !== 96) {
          return new Response(
            `Invalid RawData length ${raw.byteLength}`,
            { status: 400 }
          );
        }

        const guid = new Uint8Array(body.guid);

        // ---- upsert without REPLACE ----
        const update = await env.DB
          .prepare(
            `UPDATE historyData
             SET RawData = ?
             WHERE Guid = ? AND Datetime = ?`
          )
          .bind(raw, guid, body.datetime)
          .run();

        if (update.meta.changes === 0) {
          await env.DB
            .prepare(
              `INSERT INTO historyData (Guid, Datetime, RawData)
               VALUES (?, ?, ?)`
            )
            .bind(guid, body.datetime, raw)
            .run();
        }

        return new Response(
          JSON.stringify({ ok: true }),
          { headers: { "content-type": "application/json" } }
        );
      }

      // ===============================
      // POST /query-history
      // ===============================
      if (request.method === "POST" && url.pathname === "/query-history") {
        const body = await request.json();

        if (!Array.isArray(body.guid) || body.guid.length !== 16) {
          return new Response("Invalid guid", { status: 400 });
        }

        if (typeof body.datetime !== "number") {
          return new Response("Invalid datetime", { status: 400 });
        }

        const guid = new Uint8Array(body.guid);

        const row = await env.DB
          .prepare(
            `SELECT RawData
             FROM historyData
             WHERE Guid = ? AND Datetime = ?`
          )
          .bind(guid, body.datetime)
          .first();

        if (!row || !row.RawData) {
          return new Response(
            JSON.stringify({ hours: [] }),
            { status: 404, headers: { "content-type": "application/json" } }
          );
        }

        const buf =
          row.RawData instanceof ArrayBuffer
            ? row.RawData
            : row.RawData.buffer;

        const hours = decodeDailyRawData(buf);

        return new Response(
          JSON.stringify({
            datetime: body.datetime,
            hours
          }),
          { headers: { "content-type": "application/json" } }
        );
      }

      // ===============================
      // fallback
      // ===============================
      return new Response("Not Found", { status: 404 });

    } catch (err: any) {
      return new Response(
        `Worker error: ${err?.message ?? err}`,
        { status: 500 }
      );
    }
  }
};
