// @vitest-environment happy-dom
import { render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router";
import { describe, expect, it } from "vitest";
import fleet from "../fixtures/fleet.json";
import meta from "../fixtures/meta.json";
import { FleetView } from "./Fleet";

describe("FleetView", () => {
  it("lists visible projects with severity counts and links", () => {
    render(<MemoryRouter><FleetView fleet={fleet} meta={meta} /></MemoryRouter>);
    expect(screen.getByRole("link", { name: "healthcheck" }).getAttribute("href")).toContain("/projects/11111111");
    expect(screen.getByRole("columnheader", { name: /critical/i })).toBeTruthy();
    expect(screen.getByText(fleet.projects[0].critical)).toBeTruthy();
  });

  it("edges a project row by its worst severity, critical or warning only", () => {
    const projects = [
      { ...fleet.projects[0], project_id: "a", project_name: "a", critical: "2", warning: "1" },
      { ...fleet.projects[0], project_id: "b", project_name: "b", critical: "0", warning: "3" },
      { ...fleet.projects[0], project_id: "c", project_name: "c", critical: "0", warning: "0", info: "4" },
    ];
    render(<MemoryRouter><FleetView fleet={{ projects }} meta={meta} /></MemoryRouter>);
    const sev = (name: string) => screen.getByRole("link", { name }).closest("tr")!.getAttribute("data-severity");
    expect([sev("a"), sev("b"), sev("c")]).toEqual(["critical", "warning", "neutral"]);
  });

  it("explains access when no projects are visible", () => {
    render(<MemoryRouter><FleetView fleet={{ projects: [] }} meta={meta} /></MemoryRouter>);
    expect(screen.getByText(/no Lakebase projects you can see/i)).toBeTruthy();
    expect(screen.getByText(/Postgres role/)).toBeTruthy();
  });
});
