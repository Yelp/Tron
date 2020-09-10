import React, { useState, useEffect } from 'react';
import './NavBar.css';
import {
  Link,
} from 'react-router-dom';
import Autosuggest from 'react-autosuggest';
import { fetchFromApi } from '../../utils/utils';

function jobLink(jobName) {
  return <Link className="stretched-link" to={`/job/${jobName}`}>{jobName}</Link>;
}

function NavBar() {
  const [inputSuggestions, setInputSuggestions] = useState([]);
  const [inputValue, setInputValue] = useState('');
  const [jobData, setJobData] = useState(undefined);
  const maxSuggestions = 10;

  useEffect(() => fetchFromApi('/api/jobs', setJobData), []);

  let jobNames = [];
  if (jobData !== undefined) {
    if (!('error' in jobData)) {
      jobNames = jobData.jobs.map((job) => job.name);
    }
  }

  function getSuggestedJobNames(string) {
    const trimmed = string.trim().toLowerCase();
    const { length } = string;
    return length === 0 ? [] : jobNames
      .filter((jobName) => jobName.toLowerCase().slice(0, length) === trimmed)
      .slice(0, maxSuggestions);
  }

  const inputProps = {
    placeholder: 'Search jobs',
    value: inputValue,
    onChange: (event, { newValue }) => { setInputValue(newValue); },
    className: 'form-control',
    id: 'autosuggest-input',
  };

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
            <Autosuggest
              focusInputOnSuggestionClick={false}
              suggestions={inputSuggestions}
              onSuggestionsFetchRequested={
                (input) => setInputSuggestions(getSuggestedJobNames(input.value))
              }
              onSuggestionsClearRequested={() => setInputSuggestions([])}
              getSuggestionValue={(suggestion) => suggestion}
              renderSuggestion={jobLink}
              inputProps={inputProps}
            />
          </div>
        </form>
      </div>
    </nav>
  );
}

export default NavBar;
