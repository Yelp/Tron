/// <reference types="vite/client" />

declare module "cytoscape-dagre" {
  import type cytoscape from "cytoscape";
  const cytoscapeDagre: cytoscape.Ext;
  export default cytoscapeDagre;
}
