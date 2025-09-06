/**
 * @typedef {import("/modules/database.js").Feed} Feed
 * @typedef {import("/modules/database.js").Database} Database
 * @typedef {import("/modules/prefs.js").Prefs} Prefs
 * @typedef {import("/modules/utils.js").Comm} Comm
 */

const BATCH_SIZE = 500;
const MS_PER_DAY = 86400000;

/**
 * Processes a batch of deletions in a single database transaction.
 * @param {any[]} batch - The batch of entry objects to delete.
 * @param {IDBDatabase} idb
 * @param {Set<string>} affectedFeeds - A Set to collect the affected feed IDs.
 */
async function processDeletionBatch(batch, idb, affectedFeeds) {
    const tx = idb.transaction(['entries', 'revisions'], 'readwrite');
    const entryStore = tx.objectStore('entries');
    const revisionStore = tx.objectStore('revisions');

    for (const entry of batch) {
        affectedFeeds.add(entry.feedID);
        entryStore.delete(entry.id);
        if (entry.revisions) {
            for (const rev of entry.revisions) {
                revisionStore.delete(rev.id);
            }
        }
    }

    await new Promise((resolve, reject) => {
        tx.oncomplete = resolve;
        tx.onerror = () => reject(tx.error);
    });
}

/**
 * Permanently deletes entries and their revisions in batches.
 * @param {any[]} entriesToDelete
 * @param {IDBDatabase} idb
 * @returns {Promise<Set<string>>} A Set of affected feed IDs.
 */
async function deleteInBatches(entriesToDelete, idb) {
    const totalToDelete = entriesToDelete.length;
    const affectedFeeds = new Set();

    if (totalToDelete === 0) {
        return affectedFeeds;
    }

    const totalRevisions = entriesToDelete.reduce((sum, entry) => sum + (entry.revisions ? entry.revisions.length : 0), 0);
    console.log(`Preparing to permanently delete ${totalToDelete} entries and ${totalRevisions} associated revisions.`);

    for (let i = 0; i < totalToDelete; i += BATCH_SIZE) {
        const batch = entriesToDelete.slice(i, i + BATCH_SIZE);
        await processDeletionBatch(batch, idb, affectedFeeds);
        console.log(`Batch delete progress: ${i + batch.length}/${totalToDelete} entries processed.`);
    }
    return affectedFeeds;
}

/**
 * Calculates the expiration timestamp for each feed based on global and per-feed settings.
 * @param {Feed[]} feeds
 * @param {Prefs} prefs
 * @returns {Object<string, number>} An object mapping feedID to its expiration timestamp.
 */
function getFeedExpirationDates(feeds, prefs) {
    const feedExpirationDates = {};
    const now = Date.now();
    const globalExpire = prefs.get('database.expireEntries');
    const globalAge = prefs.get('database.entryExpirationAge');

    for (const feed of feeds) {
        let retentionDays = 0;
        if (feed.entryAgeLimit > 0) {
            retentionDays = feed.entryAgeLimit;
        } else if (globalExpire) {
            retentionDays = globalAge;
        }
        if (retentionDays > 0) {
            feedExpirationDates[feed.feedID] = now - (retentionDays * MS_PER_DAY);
        }
    }
    return feedExpirationDates;
}

/**
 * Finds entries that are soft-deleted and older than their retention period.
 * @param {{db: Database, prefs: Prefs}} modules
 */
async function getExpiredSoftDeletedEntries({ db, prefs }) {
    const deletedEntries = await db.query({ deleted: 'deleted' }).getEntries();
    console.log(`Found ${deletedEntries.length} 'deleted' entries to check for expiration.`);

    const feedExpirationDates = getFeedExpirationDates(db.feeds, prefs);

    const expiredEntries = deletedEntries.filter(entry => {
        const expirationDate = feedExpirationDates[entry.feedID];
        // If the feed has no retention period (expirationDate is undefined), it's eligible for deletion.
        if (expirationDate === undefined) {
            return true;
        }
        // Otherwise, check if the entry has expired.
        return entry.date < expirationDate;
    });
    console.log(`  - Found ${expiredEntries.length} expired entries to be permanently deleted.`);
    return expiredEntries;
}

/**
 * @param {{feeds: Feed[], db: Database}} modules
 */
async function getEntriesFromUnsubscribedFeeds({ feeds, db }) {
    let entriesFromDeletedFeeds = [];
    if (feeds.length > 0) {
        for (const feed of feeds) {
            const feedQuery = { feeds: [feed.feedID] };
            const entries = await db.query(feedQuery).getEntries();
            if (entries.length > 0) {
                entriesFromDeletedFeeds.push(...entries);
            }
        }
        console.log(`  - Found ${entriesFromDeletedFeeds.length} entries from ${feeds.length} unsubscribed feeds to be permanently deleted.`);
    }
    return entriesFromDeletedFeeds;
}

/**
 * @param {Feed[]} feedsToDelete
 * @param {Database} db
 */
async function deleteFeedObjects(feedsToDelete, db) {
    if (feedsToDelete.length === 0) {
        return;
    }
    console.log(`Permanently deleting ${feedsToDelete.length} unsubscribed feeds...`);
    const feedIdsToDelete = feedsToDelete.map(f => f.feedID);
    await db.permanentlyDeleteFeeds(feedIdsToDelete);
}

/**
 * @param {{db: Database, prefs: Prefs, comm: Comm}} modules
 */
export async function runDatabaseCleanup({ db, prefs, comm }) {
    console.log('Starting background database cleanup...');

    try {
        // --- Step 1: Identify and "commit" the deletion of feed objects
        const feedsToDelete = db.feeds.filter(f => f.hidden);
        await deleteFeedObjects(feedsToDelete, db);

        // --- Step 2: Identify all entries to be deleted
        const entriesFromDeletedFeeds = await getEntriesFromUnsubscribedFeeds({ feeds: feedsToDelete, db });
        const expiredEntries = await getExpiredSoftDeletedEntries({ db, prefs });

        // --- Step 3: Delete all identified entries
        const allEntriesToDelete = Array.from(new Map([...expiredEntries, ...entriesFromDeletedFeeds].map(e => [e.id, e])).values());
        const affectedFeeds = await deleteInBatches(allEntriesToDelete, db.db());

        if (affectedFeeds.size > 0) {
            comm.broadcast('entries-updated', {
                feeds: Array.from(affectedFeeds),
                entries: allEntriesToDelete,
                changes: { deleted: 'hard-deleted' },
            });
        }

        console.log('Background database cleanup finished successfully.');
    } catch (error) {
        console.error('An error occurred during background database cleanup:', error);
    }
}