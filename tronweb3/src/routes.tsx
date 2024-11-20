import { createRootRoute, createRoute, createRouter, Outlet } from '@tanstack/react-router';
import Dashboard from './Dashboard';
import Job from './components/Job/Job';
import App from './App';

const rootRoute = createRootRoute({
  component: App,
})

const indexRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/',
  component: Dashboard,
});

const jobRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/job/$jobId',
  component: Job,
})

const routeTree = rootRoute.addChildren()

const router = createRouter({  routeTree });

declare module '@tanstack/react-router' {
  interface Register {
    // This infers the type of our router and registers it across your entire project
    router: typeof router
  }
}

export default router;