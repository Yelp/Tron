import { useNavigate } from "react-router-dom";
import { useConfigs } from "@/lib/queries";
import { DataTable, type Column } from "@/components/DataTable";
import type { ConfigNamespace } from "@/lib/types";

export function ConfigList() {
  const { data: configs, isLoading } = useConfigs();
  const navigate = useNavigate();

  if (isLoading) {
    return <div className="text-muted-foreground">Loading...</div>;
  }

  const columns: Column<ConfigNamespace>[] = [
    {
      key: "name",
      header: "Namespace",
      render: (val) => (
        <span className="font-medium text-primary">{String(val)}</span>
      ),
    },
  ];

  return (
    <div className="space-y-4">
      <h1 className="text-xl font-bold">Configuration</h1>
      <DataTable
        columns={columns}
        data={configs ?? []}
        onRowClick={(config) => navigate(`/config/${config.name}`)}
      />
    </div>
  );
}
