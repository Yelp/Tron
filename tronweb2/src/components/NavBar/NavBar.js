import React from 'react';
import './NavBar.css';
import {
  Link,
} from 'react-router-dom';

function NavBar() {
  return (
    <nav className="navbar navbar-dark navbar-expand-sm mb-3 sticky-top">
      <Link className="navbar-brand" to="/">tronweb</Link>
      <div className="container">
        <ul className="navbar-nav">
          <li className="nav-item">
            <Link className="nav-link" to="/">
              <i className="icon-th" />
              Dashboard
            </Link>
          </li>
          <li className="nav-item">
            <Link className="nav-link" to="/configs">
              <i className="icon-wrench" />
              Config
            </Link>
          </li>
        </ul>

        <form className="form-inline">
          <div className="input-group">
            <div className="input-group-prepend">
              <span className="input-group-text" id="basic-addon1"><i className="icon-search" /></span>
            </div>
            <input type="text" className="form-control search-query typeahead" placeholder="Search" autoComplete="off" data-provide="typeahead" />
          </div>
        </form>
      </div>
    </nav>
  );
}

export default NavBar;
