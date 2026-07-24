import { BrowserRouter, Navigate, Route, Routes, useSearchParams } from "react-router-dom";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { AppShell } from "./components/AppShell";
import Today from "./pages/Today";
import Kanban from "./pages/Kanban";
import Analytics from "./pages/Analytics";
import Review from "./pages/Review";
import Explore from "./pages/Explore";

// Old routes (matrix/table/companies/changes) redirect here now that Explore
// consolidates them (see plans/phase2-spec.md "Routing / nav"). Preserves any
// existing search params (so old `?job=` deep links survive) and merges in
// the extras that recreate the old view's mode.
function RedirectPreservingParams({
  to,
  merge,
}: {
  to: string;
  merge?: Record<string, string>;
}) {
  const [params] = useSearchParams();
  const next = new URLSearchParams(params);
  if (merge) {
    for (const [key, value] of Object.entries(merge)) {
      next.set(key, value);
    }
  }
  const search = next.toString();
  return <Navigate to={search ? `${to}?${search}` : to} replace />;
}

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: 1,
      refetchOnWindowFocus: false,
    },
  },
});

export default function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <BrowserRouter>
        <Routes>
          <Route element={<AppShell />}>
            <Route index element={<Navigate to="/today" replace />} />
            <Route path="today" element={<Today />} />
            <Route path="explore" element={<Explore />} />
            <Route path="matrix" element={<RedirectPreservingParams to="/explore" />} />
            <Route path="kanban" element={<Kanban />} />
            <Route path="table" element={<RedirectPreservingParams to="/explore" />} />
            <Route
              path="companies"
              element={<RedirectPreservingParams to="/explore" merge={{ group: "company" }} />}
            />
            <Route
              path="changes"
              element={<RedirectPreservingParams to="/explore" merge={{ diff: "new" }} />}
            />
            <Route path="analytics" element={<Analytics />} />
            <Route path="review" element={<Review />} />
            <Route path="*" element={<Navigate to="/today" replace />} />
          </Route>
        </Routes>
      </BrowserRouter>
    </QueryClientProvider>
  );
}
