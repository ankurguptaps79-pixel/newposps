/* Firebase Cloud Sync for Pradeep Sales POS
   – Runs in background, IndexedDB stays primary
   – Syncs products, customers, sales, invoiceItems, loyalty, settings to Firestore
   – REAL-TIME listeners: changes on one device appear on all others instantly
   – Uses a device ID so the listener ignores its own writes
*/
'use strict';

const FIRE_CFG = {
  apiKey: "AIzaSyB1Vra5dN8MKrOkT_TigjagM4LFlXhPfTo",
  authDomain: "pradeep-pos.firebaseapp.com",
  projectId: "pradeep-pos",
  storageBucket: "pradeep-pos.firebasestorage.app",
  messagingSenderId: "130205960665",
  appId: "1:130205960665:web:52f867ef344704690eb1c8"
};

/* ================= globals ================= */
let fireDb = null, syncReady = false, syncPaused = false;
let deviceId = localStorage.getItem('ps_device_id');
if (!deviceId) { deviceId = 'dev_' + Date.now() + '_' + Math.random().toString(36).slice(2, 8); localStorage.setItem('ps_device_id', deviceId); }

const SYNC_STORES = ['products', 'customers', 'sales', 'invoiceItems', 'loyalty'];
const syncQueue = [];
let flushing = false;
// Track IDs we just wrote so the listener doesn't echo them back
const recentLocalWrites = new Map();   // key: "store/id" → timestamp

/* ================= status ================= */
let lastSync = +localStorage.getItem('ps_last_sync') || 0;
let syncStatus = 'idle';
const syncListeners = [];
function onSyncStatus(fn) { syncListeners.push(fn); }
function setSyncStatus(s) { syncStatus = s; syncListeners.forEach(fn => fn(s)); }
function getLastSync() { return lastSync; }

/* ================= init ================= */
async function initFirebase() {
  try {
    if (typeof firebase === 'undefined') { console.warn('Firebase SDK not loaded'); return; }
    firebase.initializeApp(FIRE_CFG);
    fireDb = firebase.firestore();
    try { await fireDb.enablePersistence({ synchronizeTabs: true }); } catch (e) {
      if (e.code === 'unimplemented') console.log('Firestore persistence not available');
    }
    syncReady = true;
    setSyncStatus('idle');
    console.log('Firebase sync ready, device:', deviceId);
    // Start real-time listeners after a short delay (let the app finish loading)
    setTimeout(startListeners, 1500);
  } catch (err) {
    console.error('Firebase init failed:', err);
    setSyncStatus('error');
  }
}

/* ================= REAL-TIME LISTENERS ================= */
// Listen for changes from OTHER devices and merge into local IndexedDB
const activeListeners = [];

function startListeners() {
  if (!syncReady || !fireDb) return;
  for (const store of SYNC_STORES) {
    const unsub = fireDb.collection(store).onSnapshot(snapshot => {
      snapshot.docChanges().forEach(change => {
        // Skip changes that came from this device (our own writes echoing back)
        const data = change.doc.data();
        const writeKey = store + '/' + change.doc.id;
        const recentWrite = recentLocalWrites.get(writeKey);
        if (recentWrite && Date.now() - recentWrite < 5000) return;  // skip our own write
        if (data._deviceId === deviceId) {
          // Could be our write — check if very recent
          if (data._updatedAt && data._updatedAt.toMillis && Date.now() - data._updatedAt.toMillis() < 3000) return;
        }

        if (change.type === 'added' || change.type === 'modified') {
          applyRemoteChange(store, change.doc.id, data);
        } else if (change.type === 'removed') {
          applyRemoteDelete(store, change.doc.id);
        }
      });
    }, err => {
      console.warn('Listener error on', store, err);
    });
    activeListeners.push(unsub);
  }
  console.log('Real-time listeners active on', SYNC_STORES.length, 'collections');
}

async function applyRemoteChange(store, docId, data) {
  if (!db) return;
  try {
    // Clean Firestore metadata
    delete data._deviceId;
    delete data._updatedAt;
    // Restore numeric ID (IndexedDB uses autoIncrement numbers)
    if (data.id != null && typeof data.id === 'string') {
      const parsed = parseInt(data.id);
      if (!isNaN(parsed)) data.id = parsed;
    }
    // Write to IndexedDB directly (bypass the sync hook to avoid echo)
    syncPaused = true;
    const t = db.transaction(store, 'readwrite'), done = txDone(t);
    t.objectStore(store).put(data);
    await done;
    syncPaused = false;
    // Refresh in-memory arrays so the UI updates
    await refreshStore(store);
    lastSync = Date.now();
    localStorage.setItem('ps_last_sync', lastSync);
  } catch (e) {
    syncPaused = false;
    console.warn('Remote apply failed:', store, docId, e);
  }
}

async function applyRemoteDelete(store, docId) {
  if (!db) return;
  try {
    const numId = parseInt(docId);
    const key = isNaN(numId) ? docId : numId;
    syncPaused = true;
    const t = db.transaction(store, 'readwrite'), done = txDone(t);
    t.objectStore(store).delete(key);
    await done;
    syncPaused = false;
    await refreshStore(store);
  } catch (e) {
    syncPaused = false;
    console.warn('Remote delete failed:', store, docId, e);
  }
}

// Refresh the in-memory arrays and re-render the current view
let refreshTimer;
async function refreshStore(store) {
  // Debounce: if many changes arrive at once, batch the refresh
  clearTimeout(refreshTimer);
  refreshTimer = setTimeout(async () => {
    try {
      if (store === 'products' || store === 'invoiceItems') {
        await loadProducts();
        renderTiles();
        if (typeof view !== 'undefined' && view === 'products') renderProducts();
      }
      if (store === 'customers') {
        await loadCustomers();
        if (typeof view !== 'undefined' && view === 'customers') renderCustomers();
      }
      if (store === 'loyalty') {
        await loadLoyalty();
      }
      if (store === 'sales') {
        if (typeof view !== 'undefined' && view === 'history') renderHistory();
        if (typeof view !== 'undefined' && view === 'analytics') renderAnalytics();
      }
    } catch (e) {
      console.warn('Refresh after sync failed:', e);
    }
  }, 300);
}

/* ================= queue & flush (local → cloud) ================= */
function queueSync(action, store, id, data) {
  if (!syncReady || syncPaused) return;
  // Mark this write so the listener ignores the echo
  recentLocalWrites.set(store + '/' + id, Date.now());
  // Clean old entries
  if (recentLocalWrites.size > 200) {
    const cutoff = Date.now() - 10000;
    for (const [k, t] of recentLocalWrites) { if (t < cutoff) recentLocalWrites.delete(k); }
  }
  syncQueue.push({ action, store, id, data, ts: Date.now() });
  scheduleFlush();
}

let flushTimer;
function scheduleFlush() { clearTimeout(flushTimer); flushTimer = setTimeout(flushQueue, 500); }

async function flushQueue() {
  if (flushing || !syncReady || !syncQueue.length) return;
  flushing = true;
  setSyncStatus('syncing');
  try {
    const batch = fireDb.batch();
    let count = 0;
    while (syncQueue.length && count < 400) {
      const job = syncQueue.shift();
      const docRef = fireDb.collection(job.store).doc(String(job.id));
      if (job.action === 'put') {
        const clean = sanitize(job.data);
        clean._deviceId = deviceId;
        clean._updatedAt = firebase.firestore.FieldValue.serverTimestamp();
        batch.set(docRef, clean, { merge: true });
      } else {
        batch.delete(docRef);
      }
      count++;
    }
    if (count) {
      await batch.commit();
      lastSync = Date.now();
      localStorage.setItem('ps_last_sync', lastSync);
    }
    setSyncStatus('idle');
  } catch (err) {
    console.error('Sync flush error:', err);
    setSyncStatus('error');
  }
  flushing = false;
  if (syncQueue.length) scheduleFlush();
}

function sanitize(obj) {
  const o = {};
  for (const [k, v] of Object.entries(obj)) {
    if (v === undefined || typeof v === 'function') continue;
    if (v && typeof v === 'object' && !Array.isArray(v) && !(v instanceof Date)) o[k] = sanitize(v);
    else o[k] = v;
  }
  return o;
}

/* ================= full upload (local → cloud) ================= */
async function fullUpload() {
  if (!syncReady || !db) return;
  setSyncStatus('syncing');
  try {
    for (const store of SYNC_STORES) {
      const all = await dbAll(store);
      for (let i = 0; i < all.length; i += 400) {
        const batch = fireDb.batch();
        const chunk = all.slice(i, i + 400);
        for (const item of chunk) {
          recentLocalWrites.set(store + '/' + item.id, Date.now());
          const docRef = fireDb.collection(store).doc(String(item.id));
          const clean = sanitize(item);
          clean._deviceId = deviceId;
          clean._updatedAt = firebase.firestore.FieldValue.serverTimestamp();
          batch.set(docRef, clean, { merge: true });
        }
        await batch.commit();
      }
    }
    // sync settings
    const settings = await dbAll('settings');
    if (settings.length) {
      const batch = fireDb.batch();
      for (const s of settings) {
        if (s.key === 'draft') continue;
        const docRef = fireDb.collection('settings').doc(s.key);
        batch.set(docRef, { key: s.key, value: typeof s.value === 'object' ? JSON.stringify(s.value) : s.value, _deviceId: deviceId, _updatedAt: firebase.firestore.FieldValue.serverTimestamp() }, { merge: true });
      }
      await batch.commit();
    }
    lastSync = Date.now();
    localStorage.setItem('ps_last_sync', lastSync);
    setSyncStatus('idle');
    return true;
  } catch (err) {
    console.error('Full upload error:', err);
    setSyncStatus('error');
    return false;
  }
}

/* ================= full download (cloud → local) ================= */
async function fullDownload() {
  if (!syncReady || !db) return;
  setSyncStatus('syncing');
  try {
    syncPaused = true;  // pause so dbPut doesn't echo back to cloud
    for (const store of SYNC_STORES) {
      const snap = await fireDb.collection(store).get();
      if (snap.empty) continue;
      for (const doc of snap.docs) {
        const data = doc.data();
        delete data._deviceId; delete data._updatedAt;
        if (data.id && typeof data.id === 'string') data.id = parseInt(data.id) || data.id;
        const t = db.transaction(store, 'readwrite'), done = txDone(t);
        t.objectStore(store).put(data);
        await done;
      }
    }
    syncPaused = false;
    await Promise.all([loadProducts(), loadCustomers(), loadLoyalty()]);
    renderTiles();
    lastSync = Date.now();
    localStorage.setItem('ps_last_sync', lastSync);
    setSyncStatus('idle');
    return true;
  } catch (err) {
    syncPaused = false;
    console.error('Full download error:', err);
    setSyncStatus('error');
    return false;
  }
}

/* ================= sync settings ================= */
function syncSetting(key, value) {
  if (!syncReady || syncPaused || key === 'draft') return;
  const docRef = fireDb.collection('settings').doc(key);
  const v = typeof value === 'object' ? JSON.stringify(value) : value;
  docRef.set({ key, value: v, _deviceId: deviceId, _updatedAt: firebase.firestore.FieldValue.serverTimestamp() }, { merge: true }).catch(e => console.warn('Setting sync fail:', e));
}

/* ================= hook into app ================= */
function onLocalPut(store, data) { if (data && data.id != null) queueSync('put', store, data.id, data); }
function onLocalDel(store, id) { queueSync('del', store, id, null); }
