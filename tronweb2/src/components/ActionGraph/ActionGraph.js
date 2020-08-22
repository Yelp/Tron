import React from "react";
import "./ActionGraph.css";
import {Tooltip, OverlayTrigger} from 'react-bootstrap';
import { scaleLinear } from 'd3-scale';

function setNodeLevel(action, actionMap) {
    if ("level" in action) {
        return action.level;
    }
    if (action.dependencies.length === 0) {
        action.level = 0;
        return 0;
    }
    var dependencyLevels = action.dependencies.map((actionName) => {
        return setNodeLevel(actionMap[actionName], actionMap);
    });
    var result = Math.max(...dependencyLevels) + 1;
    action.level = result;
    return result;
}

function setNodeCol(action, actionData, nextColPerLevel) {
    if ("col" in action) {
        return;
    }
    action.col = nextColPerLevel[action.level];
    nextColPerLevel[action.level] += 1;
    action.dependencies.forEach((actionName) => {
        setNodeCol(actionData[actionName], actionData, nextColPerLevel);
    });
}

function shortenName(name) {
    if (name.length > 20) {
        return name.substring(0, 20) + "...";
    } else {
        return name;
    }
}

function buildActionData(actionList) {
    var actionMap = {};
    actionList.forEach((action) => {
        var type = "internal";
        if (action["name"].includes(".")) {
            type = "external";
        }
        actionMap[action["name"]] = {
            "name": action["name"],
            "command": action["command"],
            "dependencies": action["dependencies"],
            "type": type,
        };
    });

    Object.values(actionMap).forEach((action) => {
        setNodeLevel(action, actionMap);
    });

    var nodesByLevel = [];
    Object.values(actionMap).forEach((action) => {
        if (nodesByLevel[action.level] !== undefined) {
            nodesByLevel[action.level].push(action);
        } else {
            nodesByLevel[action.level] = [action];
        }
    });

    var nextColPerLevel = new Array(nodesByLevel.length).fill(0);
    for (let i = nodesByLevel.length - 1; i >= 0; i--) {
        nodesByLevel[i].forEach((action) => {
            setNodeCol(action, actionMap, nextColPerLevel);
        });
    }

    var numCols = Math.max(...nodesByLevel.map((list) => {return list.length}));
    var numLevels = nodesByLevel.length - 1;

    return [actionMap, numCols, numLevels];
}

function buildGraph(actionData, height, width) {
    var [actionMap, numCols, numLevels] = actionData;
    var nodeList = Object.values(actionMap);
    var nodeYScale = scaleLinear()
        .domain([0, numLevels])
        .range([0, height - 30]);
    var nodeXScale = scaleLinear()
        .domain([0, numCols])
        .range([0, width]);

    var nodeElements = nodeList.map(d => (
      <OverlayTrigger placement="top" overlay={<Tooltip><div>{d.name}</div></Tooltip>}>
      <g className="node" key={d.name}>
         <circle r="6" cx={nodeXScale(d.col)} cy={nodeYScale(d.level)} />
        <text dx={nodeXScale(d.col) + 10} dy={nodeYScale(d.level)} className={d.type}>{shortenName(d.name)}</text>
      </g>
      </OverlayTrigger>
    ));

    var links = []
	nodeList.forEach((action) => {
		action.dependencies.forEach((actionName) => {
			links.push({'source': actionMap[actionName], 'dest': action});
		});
	});
    var linkElements = links.map(d => (
      <line className="link" key={'link-' + d.source.name + '-' + d.dest.name}
        markerEnd="url(#arrow)"
        x1={nodeXScale(d.source.col)}
        y1={nodeYScale(d.source.level)}
        x2={nodeXScale(d.dest.col)}
        y2={nodeYScale(d.dest.level)}></line>
    ));

    var svg = (
       <svg height={height} width={width}>
         <defs>
           <marker id="arrow" viewBox="0 0 10 10" refX="16" refY="5"
               markerUnits="strokeWidth" markerWidth="15" markerHeight="30" orient="auto">
             <path d="M 0 2 L 10 5 L 0 8 z"></path>
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
  var actionData = buildActionData(props.graph)
  var graph = buildGraph(actionData, parseInt(props.height), parseInt(props.width));

  return (
    <div id="action-graph" className="p-3 border">
      {graph}
    </div>
  );
}

export default ActionGraph;
