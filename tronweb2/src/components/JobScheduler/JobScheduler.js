import PropTypes from 'prop-types';
import React from 'react';

function JobScheduler(props) {
  const { scheduler: { type, value, jitter } } = props;
  return (
    <span>
      {type}
      {' '}
      {value}
      {jitter}
    </span>
  );
}

JobScheduler.propTypes = {
  scheduler: PropTypes.shape({
    type: PropTypes.string.isRequired,
    value: PropTypes.string.isRequired,
    jitter: PropTypes.string.isRequired,
  }).isRequired,
};

export default JobScheduler;
