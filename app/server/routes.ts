import type express from "express";
import { HttpError, type Ctx, type createHandlers } from "./lib/handlers";

type Handlers = ReturnType<typeof createHandlers>;

const ctxOf = (req: express.Request): Ctx => ({
  token: req.header("x-forwarded-access-token") ?? undefined,
  user: req.header("x-forwarded-user") ?? undefined,
  now: new Date(),
});

function send(res: express.Response, work: () => Promise<unknown>, status = 200) {
  work()
    .then((body) => res.status(status).json(body))
    .catch((e: unknown) => {
      if (e instanceof HttpError) {
        res.status(e.status).json(e.code ? { error: e.message, code: e.code } : { error: e.message });
        return;
      }
      console.error("lakebase-health route failed", e);
      res.status(502).json({ error: "upstream error" });
    });
}

const param = (v: unknown) => (typeof v === "string" ? v : "");

export function registerRoutes(app: express.Application, h: Handlers) {
  app.get("/api/meta", (req, res) => send(res, () => h.meta(ctxOf(req))));
  app.get("/api/fleet", (req, res) => send(res, () => h.fleet(ctxOf(req))));
  app.get("/api/projects/:id", (req, res) => send(res, () => h.project(ctxOf(req), param(req.params.id))));
  app.get("/api/projects/:id/actions", (req, res) =>
    send(res, () => h.actions(ctxOf(req), param(req.params.id), req.query.state)),
  );
  app.get("/api/projects/:id/findings", (req, res) =>
    send(res, () => h.projectFindings(ctxOf(req), param(req.params.id), req.query.section)),
  );
  app.get("/api/projects/:id/queries", (req, res) =>
    send(res, () => h.queries(ctxOf(req), param(req.params.id), req.query.ids)),
  );
  app.get("/api/actions/:id", (req, res) => send(res, () => h.action(ctxOf(req), param(req.params.id))));
  // AppKit's server plugin already parses JSON bodies.
  app.post("/api/actions/:id/requests", (req, res) =>
    send(res, () => h.request(ctxOf(req), param(req.params.id), req.body as unknown), 202),
  );
  app.get("/api/findings/:fp", (req, res) => send(res, () => h.finding(ctxOf(req), param(req.params.fp))));
  app.get("/api/findings/:fp/related", (req, res) => send(res, () => h.findingRelated(ctxOf(req), param(req.params.fp))));
  app.get("/api/endpoints/:id/timeline", (req, res) =>
    send(res, () => h.timeline(ctxOf(req), param(req.params.id), req.query.from, req.query.to)),
  );
  app.get("/api/endpoints/:id/samples", (req, res) =>
    send(res, () => h.samples(ctxOf(req), param(req.params.id), req.query.at)),
  );
  app.post("/api/access/refresh", (req, res) => {
    h.refresh(ctxOf(req));
    res.status(204).end();
  });
}
