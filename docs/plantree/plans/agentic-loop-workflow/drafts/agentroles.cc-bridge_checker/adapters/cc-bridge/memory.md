# CC_BRIDGE Adapter Notes For Checker

Return node check evidence to orchestrator. Do not call raw `cc_bridge reload`,
`cc_bridge kill`, `tmux`, or mutate runtime files.

Never edit `.cc-bridge/runtime`, `.cc-bridge/agents`, lease, socket, pid, mailbox, pane,
provider-state, or tmux files directly.
