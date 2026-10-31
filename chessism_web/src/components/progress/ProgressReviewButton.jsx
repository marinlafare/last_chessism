// Dismissal is a UI acknowledgement only. Workflow-specific retention stays
// with the owner; this button never submits or cancels server work.
export default function ProgressReviewButton(props) {
  return <button {...props} type="button">Reviewed — dismiss progress</button>
}
