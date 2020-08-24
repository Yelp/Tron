import PropTypes from 'prop-types';
import React from 'react';

function JobSettings(props) {
  const { allowOverlap, queueing, allNodes } = props;
  let overlapString = 'Cancel overlapping runs';
  if (allowOverlap) {
    overlapString = 'Allow overlapping runs';
  } else if (queueing) {
    overlapString = 'Queue overlapping runs';
  }

  return (
    <ul className="list-group">
      <li className="list-group-item">{overlapString}</li>
      {allNodes && <li className="list-group-item">Runs on all nodes</li>}
    </ul>
  );
}

JobSettings.propTypes = {
  allowOverlap: PropTypes.bool,
  queueing: PropTypes.bool,
  allNodes: PropTypes.bool,
};

JobSettings.defaultProps = {
  allowOverlap: true,
  queueing: true,
  allNodes: false,
};

export default JobSettings;
