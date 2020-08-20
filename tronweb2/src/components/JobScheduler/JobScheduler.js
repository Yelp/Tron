import React from 'react';

function JobScheduler(props) {
    return (
        <span>{props.scheduler.type} {props.scheduler.value}{props.scheduler.jitter}</span>
    );
}

export default JobScheduler;
