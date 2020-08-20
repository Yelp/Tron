import React, { useEffect} from "react";
import * as d3 from "d3";
import "./ActionGraph.css";


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

    var numCols = d3.max(nodesByLevel, function(d) {return d.length});
    var numLevels = nodesByLevel.length - 1;

    return [actionMap, numCols, numLevels];
}

function buildGraph(actionData, height, width) {
    var [actionMap, numCols, numLevels] = actionData;
    var nodeList = Object.values(actionMap);
    var nodeYScale = d3.scaleLinear()
        .domain([0, numLevels])
        .range([0, height - 30]);
    var nodeXScale = d3.scaleLinear()
        .domain([0, numCols])
        .range([0, width]);

    var nodes = d3.select("svg")
      .append("g")
      .attr("class", "nodes")
      .attr("transform", "translate(10, 15)")
      .selectAll("node")
      .data(nodeList)
      .enter()
      .append("g")
      .classed("node", true)
    nodes.append("circle")
      .attr("r", 4)
      .attr("cx", function(d) {return nodeXScale(d.col)})
      .attr("cy", function(d) {return nodeYScale(d.level)})
    nodes.append("text")
      .text(function(d) { return d.name })
      .attr("dx", function(d) {return nodeXScale(d.col) + 10;})
      .attr("dy", function(d) {return nodeYScale(d.level)})
      .attr("class", function(d) {return d.type});

	d3.select("svg")
      .append("defs")
	  .append("marker")
      .attr("id", "arrow")
      .attr("viewBox", "0 0 10 10")
      .attr("refX", 16)
      .attr("refY", 5)
      .attr("markerUnits", "strokeWidth")
      .attr("markerWidth",  15)
      .attr("markerHeight", 30)
      .attr("orient", "auto")
      .append("svg:path")
      .attr("d", "M 0 2 L 10 5 L 0 8 z")

    var links = []
	nodeList.forEach((action) => {
		action.dependencies.forEach((actionName) => {
			links.push({'source': actionMap[actionName], 'dest': action});
		});
	});

	d3.select("svg")
      .append("g")
      .attr("class", "links")
      .attr("transform", "translate(10, 15)")
      .selectAll("link")
      .data(links)
      .enter()
	  .append("line")
      .attr("class", "link")
      .attr("marker-end", "url(#arrow)")
      .attr("x1", function(d) {return nodeXScale(d.source.col)})
      .attr("y1", function(d) {return nodeYScale(d.source.level)})
      .attr("x2", function(d) {return nodeXScale(d.dest.col)})
      .attr("y2", function(d) {return nodeYScale(d.dest.level)})
}

function ActionGraph(props) {
  useEffect(() => {
    var actionData = buildActionData(props.graph)
    buildGraph(actionData, parseInt(props.height), parseInt(props.width));
  }, [props]);

  return (
    <div id="action-graph" className="p-3 border">
      <svg height={props.height} width={props.width}>
      </svg>
    </div>
  );
}

export default ActionGraph;
