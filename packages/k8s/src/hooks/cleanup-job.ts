import { pruneJobSet, prunePods, pruneSecrets } from '../k8s'
import { getJobSetName } from './constants'

export async function cleanupJob(): Promise<void> {
  await Promise.all([prunePods(), pruneSecrets(), pruneJobSet(getJobSetName())])
}
