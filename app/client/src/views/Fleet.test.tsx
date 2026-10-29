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

  it("explains access when no projects are visible", () => {
    render(<MemoryRouter><FleetView fleet={{ projects: [] }} meta={meta} /></MemoryRouter>);
    expect(screen.getByText(/no Lakebase projects you can see/i)).toBeTruthy();
    expect(screen.getByText(/Postgres role/)).toBeTruthy();
  });
});
