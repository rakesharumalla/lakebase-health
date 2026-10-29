import { Alert, AlertDescription } from "@databricks/appkit-ui/react";
import { Activity, TriangleAlert } from "lucide-react";
import { createBrowserRouter, Link, NavLink, Outlet, RouterProvider } from "react-router";
import { BrandMark } from "./components/BrandMark";
import { TONE_CLASSES } from "./components/tone";
import { EmptyState } from "./components/ui";
import { fmtTime, type Meta } from "./lib/format";
import { useApi } from "./lib/useApi";
import { cn } from "./lib/utils";
import ActionsPage from "./views/Actions";
import FleetPage from "./views/Fleet";
import FindingPage from "./views/Finding";
import ProjectPage from "./views/Project";
import TimelinePage from "./views/Timeline";

function Freshness({ meta }: { meta: Meta | undefined }) {
  if (!meta) return null;
  const stale = meta.stale || !meta.lastSuccessAt;
  return (
    <span className="flex items-center gap-2 text-sm text-header-muted">
      <span
        aria-hidden
        className={cn("size-2 rounded-full", stale ? "bg-signal-stale" : "bg-signal-ok")}
      />
      {meta.lastSuccessAt ? `Data as of ${fmtTime(meta.lastSuccessAt)}` : "The health engine has not run yet"}
    </span>
  );
}

function Banner({ children }: { children: string }) {
  return (
    <Alert className={cn("rounded-none border-x-0 border-t-0 px-4 md:px-8", TONE_CLASSES.warning)}>
      <TriangleAlert />
      <AlertDescription className="text-current">{children}</AlertDescription>
    </Alert>
  );
}

function Layout() {
  const meta = useApi<Meta>("/api/meta");
  const m = meta.data;
  return (
    <div className="flex min-h-screen flex-col bg-background">
      <header className="sticky top-0 z-20 bg-header text-header-foreground shadow-sm">
        <div className="mx-auto flex h-14 max-w-[1440px] items-center gap-6 px-4 md:px-8">
          <Link to="/" className="flex items-center gap-2.5 rounded-md focus-visible:outline-none">
            <BrandMark />
            <span className="text-base font-semibold tracking-tight text-header-foreground">Lakebase health</span>
          </Link>
          <nav aria-label="Main" className="flex h-full items-stretch gap-1">
            {/* Every page sits under a project, so this section stays highlighted (NavLink "/" only matches the root). */}
            <NavLink
              to="/"
              className="flex items-center border-b-2 border-lava px-3 text-sm font-medium text-header-foreground transition-colors hover:border-coral"
            >
              Projects
            </NavLink>
          </nav>
          <div className="ml-auto">
            <Freshness meta={m} />
          </div>
        </div>
      </header>
      {m?.stale && m.lastSuccessAt && (
        <Banner>
          The health engine has not completed a run in over two schedule intervals; findings may be out of date.
        </Banner>
      )}
      {m?.accessIncomplete && (
        <Banner>Some projects could not be checked just now and are hidden; reload in a minute to see them.</Banner>
      )}
      <main className="mx-auto w-full max-w-[1440px] flex-1 px-4 py-6 md:px-8 md:py-8">
        {m && !m.lastSuccessAt ? (
          <EmptyState title="No health data yet" icon={<Activity />}>
            The health engine has not run yet. Install it with the steps in the README, then reload this page.
          </EmptyState>
        ) : (
          <Outlet />
        )}
      </main>
    </div>
  );
}

const router = createBrowserRouter([
  {
    element: <Layout />,
    children: [
      { path: "/", element: <FleetPage /> },
      { path: "/projects/:id", element: <ProjectPage /> },
      { path: "/projects/:id/actions", element: <ActionsPage /> },
      { path: "/findings/:fp", element: <FindingPage /> },
      { path: "/endpoints/:id/timeline", element: <TimelinePage /> },
    ],
  },
]);

export default function App() {
  return <RouterProvider router={router} />;
}
