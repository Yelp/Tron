import PropTypes from 'prop-types';
import React from 'react';
import './ActionGraph.css';
import { Tooltip, OverlayTrigger } from 'react-bootstrap';
import { scaleLinear } from 'd3-scale';

function setNodeLevel(action, actionMap) {
  if ('level' in action) {
    return action.level;
  }
  if (action.dependencies.length === 0) {
    action.level = 0; // eslint-disable-line no-param-reassign
    return 0;
  }
  const dependencyLevels = action.dependencies.map(
    (actionName) => setNodeLevel(actionMap[actionName], actionMap),
  );
  const result = Math.max(...dependencyLevels) + 1;
  action.level = result; // eslint-disable-line no-param-reassign
  return result;
}

function setNodeCol(action, actionData, nextColPerLevel) {
  if ('col' in action) {
    return;
  }
  action.col = nextColPerLevel[action.level]; // eslint-disable-line no-param-reassign
  nextColPerLevel[action.level] += 1; // eslint-disable-line no-param-reassign
  action.dependencies.forEach((actionName) => {
    setNodeCol(actionData[actionName], actionData, nextColPerLevel);
  });
}

function shortenName(name) {
  if (name.length > 20) {
    return `${name.substring(0, 20)}...`;
  }
  return name;
}

function extendActionData(actionList) {
  const actionMap = {};
  actionList.forEach((action) => {
    let type = 'internal';
    if (action.name.includes('.')) {
      type = 'external';
    }
    actionMap[action.name] = {
      name: action.name,
      command: action.command,
      dependencies: action.dependencies,
      type,
    };
  });

  Object.values(actionMap).forEach((action) => {
    setNodeLevel(action, actionMap);
  });

  const nodesByLevel = [];
  Object.values(actionMap).forEach((action) => {
    if (nodesByLevel[action.level] !== undefined) {
      nodesByLevel[action.level].push(action);
    } else {
      nodesByLevel[action.level] = [action];
    }
  });

  const nextColPerLevel = new Array(nodesByLevel.length).fill(0);
  for (let i = nodesByLevel.length - 1; i >= 0; i -= 1) {
    nodesByLevel[i].forEach((action) => {
      setNodeCol(action, actionMap, nextColPerLevel);
    });
  }

  const numCols = Math.max(...nodesByLevel.map((list) => list.length));
  const numLevels = nodesByLevel.length - 1;

  return [actionMap, numCols, numLevels];
}

function buildGraph(actionData, height, width) {
  const [actionMap, numCols, numLevels] = actionData;
  const nodeList = Object.values(actionMap);
  const nodeYScale = scaleLinear()
    .domain([0, numLevels])
    .range([0, height - 30]);
  const nodeXScale = scaleLinear()
    .domain([0, numCols])
    .range([0, width]);

  const nodeElements = nodeList.map((d) => (
    <OverlayTrigger key={d.name} placement="top" overlay={<Tooltip><div>{d.name}</div></Tooltip>}>
      <g className="node">
        <circle r="6" cx={nodeXScale(d.col)} cy={nodeYScale(d.level)} />
        <text dx={nodeXScale(d.col) + 10} dy={nodeYScale(d.level)} className={d.type}>
          {shortenName(d.name)}
        </text>
      </g>
    </OverlayTrigger>
  ));

  const links = [];
  nodeList.forEach((action) => {
    action.dependencies.forEach((actionName) => {
      links.push({ source: actionMap[actionName], dest: action });
    });
  });
  const linkElements = links.map((d) => (
    <line
      className="link"
      key={`link-${d.source.name}-${d.dest.name}`}
      markerEnd="url(#arrow)"
      x1={nodeXScale(d.source.col)}
      y1={nodeYScale(d.source.level)}
      x2={nodeXScale(d.dest.col)}
      y2={nodeYScale(d.dest.level)}
    />
  ));

  const svg = (
    <svg height={height} width={width}>
      <defs>
        <marker
          id="arrow"
          viewBox="0 0 10 10"
          refX="16"
          refY="5"
          markerUnits="strokeWidth"
          markerWidth="15"
          markerHeight="30"
          orient="auto"
        >
          <path d="M 0 2 L 10 5 L 0 8 z" />
        </marker>
      </defs>
      <g className="nodes" transform="translate(10, 15)">
        {nodeElements}
      </g>
      <g className="links" transform="translate(10, 15)">
        {linkElements}
      </g>
    </svg>
  );
  return svg;
}

function ActionGraph(props) {
  const { actionData, height, width } = props;
  const dataForGraph = extendActionData(actionData);
  const graph = buildGraph(dataForGraph, height, width);

  return (
    <div id="action-graph" className="p-3 border">
      {graph}
    </div>
  );
}

ActionGraph.propTypes = {
  height: PropTypes.number.isRequired,
  width: PropTypes.number.isRequired,
  actionData: PropTypes.arrayOf(PropTypes.shape({
    name: PropTypes.string.isRequired,
    command: PropTypes.string.isRequired,
    dependencies: PropTypes.arrayOf(PropTypes.string).isRequired,
  })).isRequired,
};

export default ActionGraph;
