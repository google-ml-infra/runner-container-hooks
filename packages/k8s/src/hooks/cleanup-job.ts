import {
  pruneJobSet,
  pruneJobSetNetworkPolicy,
  prunePods,
  pruneSecrets,
  pruneServices
} from '../k8s'
import { getJobSetName } from './constants'

export async function cleanupJob(): Promise<void> {
  await Promise.all([
    prunePods(),
    pruneSecrets(),
    pruneServices(),
    pruneJobSet(getJobSetName()),
    pruneJobSetNetworkPolicy(getJobSetName())
  ])
}
