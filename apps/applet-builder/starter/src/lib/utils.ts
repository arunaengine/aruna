/** Joins class names and drops empty ones; a small stand-in for clsx with tailwind-merge. */
export function cn(...inputs: unknown[]): string {
  return inputs.filter((input): input is string => typeof input === "string" && input !== "").join(" ");
}
