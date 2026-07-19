import { BrowserRouter, Navigate, Route, Routes } from "react-router-dom";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { AppShell } from "./components/AppShell";
import Today from "./pages/Today";
import Matrix from "./pages/Matrix";
import Kanban from "./pages/Kanban";
import TableView from "./pages/TableView";
import Companies from "./pages/Companies";
import Changes from "./pages/Changes";
import Analytics from "./pages/Analytics";
import Review from "./pages/Review";

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
            <Route path="matrix" element={<Matrix />} />
            <Route path="kanban" element={<Kanban />} />
            <Route path="table" element={<TableView />} />
            <Route path="companies" element={<Companies />} />
            <Route path="changes" element={<Changes />} />
            <Route path="analytics" element={<Analytics />} />
            <Route path="review" element={<Review />} />
            <Route path="*" element={<Navigate to="/today" replace />} />
          </Route>
        </Routes>
      </BrowserRouter>
    </QueryClientProvider>
  );
}
