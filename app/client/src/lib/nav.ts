/** Sub-navigation shared by a project's findings page and its fixes page. */
export function projectNav(id: string) {
  return [
    { label: "Health report", to: `/projects/${id}`, end: true },
    { label: "Fixes and approvals", to: `/projects/${id}/actions` },
  ];
}
