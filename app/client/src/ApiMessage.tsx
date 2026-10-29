import { Alert, AlertDescription, AlertTitle } from "@databricks/appkit-ui/react";
import { CircleAlert, Lock, SearchX } from "lucide-react";
import { TONE_CLASSES } from "./components/tone";
import type { ApiError } from "./lib/format";

export function ApiMessage({ error }: { error: ApiError }) {
  if (error.status === 401)
    return (
      <Alert role="alert" className={TONE_CLASSES.warning}>
        <Lock />
        <AlertTitle>Session expired</AlertTitle>
        <AlertDescription className="text-current">Your session could not be verified; reload the page.</AlertDescription>
      </Alert>
    );
  if (error.status === 404)
    return (
      <Alert role="alert" className={TONE_CLASSES.neutral}>
        <SearchX />
        <AlertTitle>Not found</AlertTitle>
        <AlertDescription className="text-current">Not found or not visible to you.</AlertDescription>
      </Alert>
    );
  return (
    <Alert role="alert" className={TONE_CLASSES.critical}>
      <CircleAlert />
      <AlertTitle>Could not load this page</AlertTitle>
      <AlertDescription className="text-current">Something went wrong: {error.message}</AlertDescription>
    </Alert>
  );
}
