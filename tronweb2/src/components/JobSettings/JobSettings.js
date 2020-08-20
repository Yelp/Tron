import React from 'react';

function JobSettings(props) {
    if (props.allowOverlap) {
        var overlapString = 'Allow overlapping runs';
    } else if (props.queueing) {
        overlapString = 'Queue overlapping runs';
    } else {
        overlapString = 'Cancel overlapping runs';
    }

    return (
        <ul className='list-group'>
          <li className='list-group-item'>{overlapString}</li>
          {props.allNodes && <li className='list-group-item'>Runs on all nodes</li>}
        </ul>
    );
}

export default JobSettings;
