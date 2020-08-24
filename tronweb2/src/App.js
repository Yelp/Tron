import React from 'react';
import {
  HashRouter as Router,
  Switch,
  Route,
} from 'react-router-dom';
import { NavBar, JobsDashboard, Job } from './components';

import './App.css';

function App() {
  return (
    <Router>
      <div className="container">
        <NavBar />
        <div className="p-3">
          <Switch>
            <Route path="/job/:jobId">
              <Job />
            </Route>
            <Route path="/configs">
              <Configs />
            </Route>
            <Route path="/">
              <JobsDashboard />
            </Route>
          </Switch>
        </div>
      </div>
    </Router>
  );
}

function Configs() {
  return <h2>Configs</h2>;
}

export default App;
