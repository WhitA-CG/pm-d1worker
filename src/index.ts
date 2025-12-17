function readInt64LE(view: DataView, offset: number): number {
  const low = view.getUint32(offset, true);
  const high = view.getUint32(offset + 4, true);
  return high * 2 ** 32 + low;
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
    try {
      const url = new URL(request.url);
      const buf = await request.arrayBuffer();
      const view = new DataView(buf);

      // ===============================
      // INSERT
      // ===============================
      if (request.method === "POST" && url.pathname === "/insert-history") {
        if (buf.byteLength !== 120) {
          return new Response("Invalid binary length", { status: 400 });
        }

        const guid = new Uint8Array(buf.slice(0, 16));
        const datetime = readInt64LE(view, 16);
        const raw = new Uint8Array(buf.slice(24));

        if (raw.byteLength !== 96) {
          return new Response("Invalid RawData", { status: 400 });
        }

        await env.DB.prepare(
          `INSERT OR REPLACE INTO historyData (Guid, Datetime, RawData)
           VALUES (?, ?, ?)`
        )
          .bind(guid, datetime, raw)
          .run();

        return new Response("OK", { status: 200 });
      }

      // ===============================
      // QUERY
      // ===============================
      if (request.method === "POST" && url.pathname === "/query-history") {
        if (buf.byteLength !== 24) {
          return new Response("Invalid binary length", { status: 400 });
        }

        const guid = new Uint8Array(buf.slice(0, 16));
        const datetime = readInt64LE(view, 16);

        const row = await env.DB.prepare(
          `SELECT RawData
           FROM historyData
           WHERE Guid = ? AND Datetime = ?`
        )
          .bind(guid, datetime)
          .first();

        if (!row || !row.RawData) {
          return new Response("Not Found", { status: 404 });
        }

        const raw =
          row.RawData instanceof Uint8Array
            ? row.RawData
            : new Uint8Array(row.RawData);

        if (raw.byteLength !== 96) {
          return new Response("Corrupted RawData", { status: 500 });
        }

        return new Response(raw, {
          status: 200,
          headers: {
            "content-type": "application/octet-stream"
          }
        });
      }

      return new Response("Not Found", { status: 404 });
    } catch (e: any) {
      return new Response(`Worker error: ${e.message}`, { status: 500 });
    }
  }
};
