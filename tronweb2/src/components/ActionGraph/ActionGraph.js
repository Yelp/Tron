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

function shortenName(name) {
  const maxTextLength = 20;
  if (name.length > maxTextLength) {
    return `${name.substring(0, maxTextLength)}...`;
  }
  return name;
}

function actionTooltip(action) {
  return (
    <Tooltip>
      <div className="tooltip-action-name p-2">
        {action.name}
      </div>
      <div className="p-2">
        command:
        {' '}
        {action.command}
      </div>
    </Tooltip>
  );
}

function buildGraph(actionData, minWidth) {
  const [actionMap, numCols, numLevels] = actionData;
  const nodeList = Object.values(actionMap);

  const actualWidth = Math.max(minWidth, numCols * 80);
  const actualHeight = (numLevels + 1) * 100;

  const rectHeight = 36;
  const rectSidePadding = 10;

  const nodeYScale = scaleLinear()
    .domain([0, numLevels])
    .range([0, actualHeight - rectHeight - 5]);
  const nodeXScale = scaleLinear()
    .domain([0, numCols])
    .range([0, actualWidth]);

  const nodeElements = nodeList.map((d) => (
    <OverlayTrigger key={d.name} placement="left" overlay={actionTooltip(d)}>
      <g className="node">
        <rect
          x={nodeXScale(d.col) - rectSidePadding}
          y={nodeYScale(d.level) - rectHeight / 2}
          rx="5"
          ry="5"
          width={(shortenName(d.name).length * 7) + (2 * rectSidePadding) + 1}
          height={rectHeight}
          stroke="black"
          fill="#ddd"
          strokeWidth="2"
          className={d.type}
        />
        <text dx={nodeXScale(d.col)} dy={nodeYScale(d.level) + 4} className={d.type}>
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
      y1={nodeYScale(d.source.level) + rectHeight / 2}
      x2={nodeXScale(d.dest.col)}
      y2={nodeYScale(d.dest.level) - rectHeight / 2}
    />
  ));

  const transform = `translate(${rectSidePadding + 1}, ${rectHeight / 2 + 1})`;
  const svg = (
    <svg height={actualHeight} width={actualWidth}>
      <defs>
        <marker
          id="arrow"
          viewBox="0 0 10 10"
          refX="10"
          refY="5"
          markerUnits="strokeWidth"
          markerWidth="15"
          markerHeight="30"
          orient="auto"
        >
          <path d="M 0 2 L 10 5 L 0 8 z" />
        </marker>
      </defs>
      <g className="links" transform={transform}>
        {linkElements}
      </g>
      <g className="nodes" transform={transform}>
        {nodeElements}
      </g>
    </svg>
  );
  return svg;
}

function ActionGraph(props) {
  const { actionData, height, width } = props;
  const dataForGraph = extendActionData(actionData);

  // The div will be up to the given height and stretch to fit horizontally
  // Overflow of the svg will scroll in both directions within the div
  const style = { maxHeight: height, width: '100%', overflow: 'scroll' };
  // Subtract a little from the width for the svg to allow for padding in the div
  const graph = buildGraph(dataForGraph, width - 50);

  return (
    <div id="action-graph" className="p-3 border" style={style}>
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
