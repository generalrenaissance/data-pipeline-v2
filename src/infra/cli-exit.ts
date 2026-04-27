export function computeExitCode(args: {
  errored: boolean;
  errorCount: number;
  allowPartial: boolean;
}): 0 | 1 {
  if (args.errored) return 1;
  if (args.errorCount > 0 && !args.allowPartial) return 1;
  return 0;
}

export function envAllowsPartial(value: string | undefined): boolean {
  if (!value) return false;
  return ['1', 'true', 'yes'].includes(value.trim().toLowerCase());
}
