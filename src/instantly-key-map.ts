export function sanitizeInstantlyKeyMap(
  keyMap: Record<string, unknown>,
): Record<string, string> {
  return Object.fromEntries(
    Object.entries(keyMap)
      .flatMap(([slug, value]) => {
        if (typeof value !== 'string') return [];
        return [[slug.trim(), value.trim()] as const];
      })
      .filter(([slug, value]) => slug.length > 0 && value.length > 0),
  );
}

export function parseInstantlyKeyMap(json: string): Record<string, string> {
  return sanitizeInstantlyKeyMap(JSON.parse(json) as Record<string, unknown>);
}
