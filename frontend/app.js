/* ═══════════════════════════════════════════════════════════════════════════
   PolicyGuard AI — Application Logic
   ═══════════════════════════════════════════════════════════════════════════ */

const API_BASE = 'http://localhost:8000/api';

// ─── State ───────────────────────────────────────────────────────────────────
const state = {
  selectedFiles: [],
  controlAreas: [],
  activeFields: new Map(),
  activeCategory: 'all',
  // Shared active collection — persists across tab switches
  activeCollection: '',
  collections: [],
};

// ─── Init ─────────────────────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
  checkHealth();
  loadCollections();       // loads collections → sets activeCollection → loads docs
  loadControlAreas();
  setInterval(checkHealth, 30_000);
});

// ─── Tab switching ────────────────────────────────────────────────────────────
function switchTab(tabName) {
  document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
  document.getElementById(`panel-${tabName}`).classList.add('active');
  document.getElementById(`tab-${tabName}`).classList.add('active');

  // Load data for the active tab
  if (tabName === 'database') {
    loadClassifiedChunks('policy');
  } else if (tabName === 'brd') {
    loadBrdDocuments();
    loadClassifiedChunks('brd');
  }
}

// ─── Health check ─────────────────────────────────────────────────────────────
async function checkHealth() {
  const dot = document.getElementById('healthDot');
  const label = document.getElementById('healthLabel');
  try {
    const data = await apiFetch('/health');
    dot.className = `health-dot ${data.overall}`;
    label.textContent = data.overall === 'ok'
      ? 'All systems operational'
      : `${data.overall}: ${data.services.filter(s => s.status !== 'ok').map(s => s.name).join(', ')}`;
    label.title = data.services.map(s => `${s.name}: ${s.detail}`).join('\n');
  } catch {
    dot.className = 'health-dot error';
    label.textContent = 'Backend offline';
  }
}

// ─── API helpers ──────────────────────────────────────────────────────────────
async function apiFetch(path, options = {}) {
  const res = await fetch(`${API_BASE}${path}`, options);
  if (!res.ok) {
    const err = await res.text();
    throw new Error(`API ${path}: ${res.status} — ${err}`);
  }
  return res.json();
}

async function apiFetchRaw(path, options = {}) {
  const res = await fetch(`${API_BASE}${path}`, options);
  if (!res.ok) {
    const err = await res.text();
    throw new Error(`${res.status} — ${err}`);
  }
  return res.json();
}

// ─── Toast ────────────────────────────────────────────────────────────────────
function showToast(message, type = 'info', duration = 4000) {
  const container = document.getElementById('toastContainer');
  const icons = {
    success: `<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round" style="color:#22c55e"><polyline points="20 6 9 17 4 12"/></svg>`,
    error:   `<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round" style="color:#ef4444"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg>`,
    warning: `<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round" style="color:#eab308"><path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>`,
    info:    `<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round" style="color:#818cf8"><circle cx="12" cy="12" r="10"/><line x1="12" y1="8" x2="12" y2="12"/><line x1="12" y1="16" x2="12.01" y2="16"/></svg>`,
  };
  const toast = document.createElement('div');
  toast.className = `toast ${type}`;
  toast.innerHTML = `${icons[type] || ''}<span>${message}</span>`;
  container.appendChild(toast);
  setTimeout(() => {
    toast.style.animation = 'toastOut 0.25s ease forwards';
    setTimeout(() => toast.remove(), 250);
  }, duration);
}

// ─── Loading overlay ──────────────────────────────────────────────────────────
function showLoading(text = 'Processing…', sub = '') {
  document.getElementById('loadingText').textContent = text;
  document.getElementById('loadingSub').textContent = sub;
  document.getElementById('loadingOverlay').classList.remove('hidden');
}
function hideLoading() {
  document.getElementById('loadingOverlay').classList.add('hidden');
}

// ══════════════════════════════════════════════════════════════════════════════
//  COLLECTION MANAGEMENT (shared bar)
// ══════════════════════════════════════════════════════════════════════════════

async function loadCollections() {
  try {
    const data = await apiFetch('/collections');
    state.collections = data.collections;

    const select = document.getElementById('collectionSelect');
    select.innerHTML = data.collections.length === 0
      ? '<option value="">No collections yet</option>'
      : data.collections.map(c =>
          `<option value="${escHtml(c)}">${escHtml(c)}</option>`
        ).join('');

    // Prefer the currently active collection if it still exists in the list,
    // otherwise fall back to the server's default, then the first in the list.
    const preferred = state.activeCollection && data.collections.includes(state.activeCollection)
      ? state.activeCollection
      : (data.default || data.collections[0] || '');

    state.activeCollection = preferred;
    if (preferred) select.value = preferred;

    // Load documents for whichever collection is now active
    await loadDocuments();
    if (state.activeCollection) {
      loadClassifiedChunks('policy');
    }
  } catch (err) {
    showToast('Failed to load collections.', 'error');
  }
}

function onCollectionChange(value) {
  if (!value) return;
  state.activeCollection = value;
  // Reload documents for the newly selected collection
  loadDocuments();
  loadBrdDocuments();
  updateTotalDocsCount();
  loadClassifiedChunks('policy');
  loadClassifiedChunks('brd');
  // Hide results from a previous analysis run (they belong to the old collection)
  document.getElementById('resultsSection').classList.add('hidden');
  showToast(`Switched to collection: ${value}`, 'info', 2500);
}

function toggleNewCollection(show) {
  const modal = document.getElementById('newCollectionModal');
  if (show) {
    modal.style.display = 'flex';
    // Small delay so the browser paints the modal before focusing
    setTimeout(() => {
      const input = document.getElementById('newCollectionInput');
      if (input) { input.value = ''; input.focus(); }
    }, 50);
  } else {
    modal.style.display = 'none';
    const input = document.getElementById('newCollectionInput');
    if (input) input.value = '';
    // Reset create button label (textContent would strip the SVG icon)
    const btn = document.getElementById('ncCreateBtn');
    if (btn) {
      btn.disabled = false;
      btn.innerHTML = '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/></svg> Create Collection';
    }
  }
}

function handleBackdropClick(e) {
  // Dismiss only when clicking the backdrop itself, not the modal box
  if (e.target === document.getElementById('newCollectionModal')) {
    toggleNewCollection(false);
  }
}

async function createNewCollection() {
  const input = document.getElementById('newCollectionInput');
  const name = input.value.trim();
  if (!name) { showToast('Please enter a collection name.', 'warning'); return; }

  const createBtn = document.getElementById('ncCreateBtn');
  if (createBtn) { createBtn.disabled = true; createBtn.textContent = 'Creating…'; }

  try {
    const data = await apiFetch('/collections', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name }),
    });

    // Set the active collection BEFORE reloading the list so loadCollections
    // picks it up and selects it in the dropdown automatically
    state.activeCollection = data.name;
    toggleNewCollection(false);   // closes modal + resets button

    const msg = data.original && data.original !== data.name
      ? `Collection created as "${data.name}" (normalised from "${data.original}")`
      : `Collection "${data.name}" created successfully.`;
    showToast(msg, 'success');

    // Reload list — will auto-select state.activeCollection and load its docs
    await loadCollections();

  } catch (err) {
    showToast(`Failed to create collection: ${err.message}`, 'error');
    // Re-enable the button on failure so user can try again
    if (createBtn) {
      createBtn.disabled = false;
      createBtn.innerHTML = '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/></svg> Create Collection';
    }
  }
}

// ══════════════════════════════════════════════════════════════════════════════
//  DATABASE TAB
// ══════════════════════════════════════════════════════════════════════════════

function handleDragOver(e) {
  e.preventDefault();
  document.getElementById('uploadZone').classList.add('drag-over');
}

function handleDragLeave(e) {
  if (!e.currentTarget.contains(e.relatedTarget)) {
    document.getElementById('uploadZone').classList.remove('drag-over');
  }
}

function handleDrop(e) {
  e.preventDefault();
  document.getElementById('uploadZone').classList.remove('drag-over');
  const files = Array.from(e.dataTransfer.files).filter(f => f.name.toLowerCase().endsWith('.pdf'));
  if (files.length === 0) { showToast('Only PDF files are supported.', 'warning'); return; }
  addFilesToQueue(files);
}

function handleFileSelect(e) {
  addFilesToQueue(Array.from(e.target.files));
  e.target.value = '';
}

function addFilesToQueue(files) {
  const existing = new Set(state.selectedFiles.map(f => f.name));
  state.selectedFiles.push(...files.filter(f => !existing.has(f.name)));
  renderFileQueue();
  document.getElementById('storeBtn').disabled = state.selectedFiles.length === 0;
}

function removeFromQueue(index) {
  state.selectedFiles.splice(index, 1);
  renderFileQueue();
  document.getElementById('storeBtn').disabled = state.selectedFiles.length === 0;
}

function clearQueue() {
  state.selectedFiles = [];
  renderFileQueue();
  document.getElementById('storeBtn').disabled = true;
}

function renderFileQueue() {
  const container = document.getElementById('fileQueue');
  if (state.selectedFiles.length === 0) { container.classList.add('hidden'); return; }
  container.classList.remove('hidden');
  container.innerHTML = state.selectedFiles.map((f, i) => `
    <div class="file-item" id="file-item-${i}">
      <div class="file-icon">
        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/></svg>
      </div>
      <span class="file-name" title="${escHtml(f.name)}">${escHtml(f.name)}</span>
      <span class="file-size">${formatBytes(f.size)}</span>
      <span class="file-status queued" id="file-status-${i}">Queued</span>
      <button class="file-remove" onclick="removeFromQueue(${i})" title="Remove">
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>
      </button>
    </div>
  `).join('');
}

function formatBytes(bytes) {
  if (bytes < 1024) return bytes + ' B';
  if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB';
  return (bytes / 1024 / 1024).toFixed(1) + ' MB';
}

// ─── Store documents ──────────────────────────────────────────────────────────
async function storeDocuments() {
  if (!state.activeCollection) {
    showToast('No collection selected. Please create or select a collection first.', 'warning');
    return;
  }
  if (state.selectedFiles.length === 0) return;

  const btn = document.getElementById('storeBtn');
  btn.disabled = true;
  showLoading(
    `Embedding & storing into "${state.activeCollection}"…`,
    'This may take a few minutes for large PDFs.'
  );

  const formData = new FormData();
  state.selectedFiles.forEach(f => formData.append('files', f));
  formData.append('collection_name', state.activeCollection);

  const activeAreasJson = [...state.activeFields.entries()].map(([id, {area, description}]) => ({
    id: id,
    name: area.name,
    label: area.label || area.name,
    placeholder: description.trim(),
    category: area.category || 'Custom'
  }));
  if (activeAreasJson.length > 0) {
    formData.append('control_areas', JSON.stringify(activeAreasJson));
  }

  try {
    const results = await apiFetchRaw('/ingest', { method: 'POST', body: formData });

    results.forEach((r, i) => {
      const statusEl = document.getElementById(`file-status-${i}`);
      if (statusEl) {
        statusEl.className = `file-status ${r.status}`;
        statusEl.textContent = r.status === 'success' ? `✓ ${r.chunks_stored} chunks` : `✗ ${r.message}`;
      }
    });

    const succeeded = results.filter(r => r.status === 'success').length;
    const failed    = results.filter(r => r.status !== 'success').length;
    if (succeeded > 0) showToast(`${succeeded} document(s) stored in "${state.activeCollection}".`, 'success');
    if (failed > 0)    showToast(`${failed} document(s) failed to ingest.`, 'error');

    await loadDocuments();
    updateTotalDocsCount();
    loadClassifiedChunks('policy');

    const failedNames = new Set(results.filter(r => r.status !== 'success').map(r => r.filename));
    state.selectedFiles = state.selectedFiles.filter(f => failedNames.has(f.name));
    renderFileQueue();
  } catch (err) {
    showToast(`Ingestion failed: ${err.message}`, 'error');
  } finally {
    hideLoading();
    btn.disabled = state.selectedFiles.length === 0;
  }
}

// ─── Load documents ───────────────────────────────────────────────────────────
async function loadDocuments() {
  if (!state.activeCollection) return;
  const container = document.getElementById('documentsContainer');
  try {
    const data = await apiFetch(`/documents?collection_name=${encodeURIComponent(state.activeCollection)}&doc_type=policy`);
    const docs = data.documents;

    if (docs.length === 0) {
      container.innerHTML = `
        <div class="empty-state">
          <svg width="40" height="40" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/></svg>
          <p>No documents in <strong>${escHtml(state.activeCollection)}</strong> yet. Upload PDFs to get started.</p>
        </div>`;
      return;
    }

    container.innerHTML = `
      <table class="documents-table">
        <thead>
          <tr>
            <th>Document</th>
            <th>Chunks</th>
            <th>Ingested At</th>
            <th>Actions</th>
          </tr>
        </thead>
        <tbody>
          ${docs.map(d => `
            <tr>
              <td>
                <div class="doc-name">
                  <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="color:#ef4444"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/></svg>
                  ${escHtml(d.filename)}
                </div>
              </td>
              <td class="doc-chunks">${d.chunk_count}</td>
              <td>${d.ingested_at ? new Date(d.ingested_at).toLocaleString('en-GB', {day:'2-digit',month:'short',year:'numeric',hour:'2-digit',minute:'2-digit'}) : '—'}</td>
              <td>
                <button class="btn btn-sm btn-danger" onclick="deleteDocument('${escHtml(d.filename)}')">
                  <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="3 6 5 6 21 6"/><path d="M19 6l-1 14H6L5 6"/><path d="M10 11v6"/><path d="M14 11v6"/><path d="M9 6V4h6v2"/></svg>
                  Delete
                </button>
              </td>
            </tr>
          `).join('')}
        </tbody>
      </table>`;
  } catch (err) {
    container.innerHTML = `<div class="empty-state"><p style="color:var(--red)">Failed to load documents: ${err.message}</p></div>`;
  }
}

// ─── Load BRD documents ───────────────────────────────────────────────────────
async function loadBrdDocuments() {
  if (!state.activeCollection) return;
  const container = document.getElementById('brdDocumentsContainer');
  try {
    const data = await apiFetch(`/documents?collection_name=${encodeURIComponent(state.activeCollection)}&doc_type=brd`);
    const docs = data.documents;

    if (docs.length === 0) {
      container.innerHTML = `
        <div class="empty-state">
          <svg width="40" height="40" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/></svg>
          <p>No BRD documents stored yet</p>
        </div>`;
      return;
    }

    container.innerHTML = `
      <table class="documents-table">
        <thead>
          <tr>
            <th>Document</th>
            <th>Chunks</th>
            <th>Ingested At</th>
            <th>Actions</th>
          </tr>
        </thead>
        <tbody>
          ${docs.map(d => `
            <tr>
              <td>
                <div class="doc-name">
                  <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="color:var(--violet)"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/></svg>
                  ${escHtml(d.filename)}
                </div>
              </td>
              <td class="doc-chunks">${d.chunk_count}</td>
              <td>${d.ingested_at ? new Date(d.ingested_at).toLocaleString('en-GB', {day:'2-digit',month:'short',year:'numeric',hour:'2-digit',minute:'2-digit'}) : '—'}</td>
              <td>
                <button class="btn btn-sm btn-danger" onclick="deleteDocument('${escHtml(d.filename)}')"> 
                  <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="3 6 5 6 21 6"/><path d="M19 6l-1 14H6L5 6"/><path d="M10 11v6"/><path d="M14 11v6"/><path d="M9 6V4h6v2"/></svg>
                  Delete
                </button>
              </td>
            </tr>
          `).join('')}
        </tbody>
      </table>`;
  } catch (err) {
    container.innerHTML = `<div class="empty-state"><p style="color:var(--red)">Failed to load BRD documents: ${err.message}</p></div>`;
  }
}

async function deleteDocument(filename) {
  if (!confirm(`Delete all chunks for "${filename}" from "${state.activeCollection}"? This cannot be undone.`)) return;
  try {
    await apiFetch(
      `/documents/${encodeURIComponent(filename)}?collection_name=${encodeURIComponent(state.activeCollection)}`,
      { method: 'DELETE' }
    );
    showToast(`"${filename}" deleted from "${state.activeCollection}".`, 'success');
    await loadDocuments();
    await loadBrdDocuments();
    updateTotalDocsCount();
  } catch (err) {
    showToast(`Delete failed: ${err.message}`, 'error');
  }
}

// ══════════════════════════════════════════════════════════════════════════════
//  ANALYSE TAB
// ══════════════════════════════════════════════════════════════════════════════

async function loadControlAreas() {
  try {
    const data = await apiFetch('/control-areas');
    state.controlAreas = data.control_areas;
    renderControlAreaList();
  } catch (err) {
    showToast('Failed to load control areas.', 'error');
  }
}

// ─── Control area list ────────────────────────────────────────────────────────
function toggleAllControlAreas(isChecked) {
  if (isChecked) {
    state.controlAreas.forEach(ca => {
      if (!state.activeFields.has(ca.id)) {
        state.activeFields.set(ca.id, { area: ca, description: '' });
      }
    });
  } else {
    state.controlAreas.forEach(ca => {
      state.activeFields.delete(ca.id);
    });
  }
  renderFields();
  renderControlAreaList();
}

function renderControlAreaList() {
  const allSelected = state.controlAreas.length > 0 && state.controlAreas.every(ca => state.activeFields.has(ca.id));
  
  const cbAnalyse = document.getElementById('selectAllCheckbox');
  if (cbAnalyse) cbAnalyse.checked = allSelected;
  const cbPolicy = document.getElementById('selectAllCheckboxPolicy');
  if (cbPolicy) cbPolicy.checked = allSelected;
  const cbBrd = document.getElementById('selectAllCheckboxBrd');
  if (cbBrd) cbBrd.checked = allSelected;

  const html = state.controlAreas.map(ca => `
    <button class="ca-item ${state.activeFields.has(ca.id) ? 'selected' : ''}"
            onclick="toggleControlArea(${JSON.stringify(ca).replace(/"/g, '&quot;')})">
      <span style="flex:1;text-align:left">${escHtml(ca.name)}</span>
    </button>
  `).join('');

  const analyseList = document.getElementById('analyseControlAreaList');
  if (analyseList) analyseList.innerHTML = html;
  
  const policyList = document.getElementById('policyControlAreaList');
  if (policyList) policyList.innerHTML = html;

  const brdList = document.getElementById('brdControlAreaList');
  if (brdList) brdList.innerHTML = html;
}

function toggleControlArea(ca) {
  state.activeFields.has(ca.id) ? removeField(ca.id) : addField(ca);
}

// ─── Custom control area ──────────────────────────────────────────────────────
function addCustomArea(inputId = 'analyseCustomAreaInput') {
  const input = document.getElementById(inputId);
  if (!input) return;
  const name = input.value.trim();
  if (!name) { showToast('Please enter a control area name.', 'warning'); return; }
  const id = 'custom_' + name.toLowerCase().replace(/\s+/g, '_') + '_' + Date.now();
  
  const newCa = { id, name, label: name, placeholder: `Describe your implementation of ${name}…`, category: 'Custom' };
  
  // Persist it locally for this session
  if (!state.controlAreas.find(c => c.id === id)) {
    state.controlAreas.push(newCa);
  }
  
  addField(newCa);
  input.value = '';
}

// ─── Field management ─────────────────────────────────────────────────────────
function addField(ca) {
  if (state.activeFields.has(ca.id)) return;
  state.activeFields.set(ca.id, { area: ca, description: '' });
  renderFields();
  renderControlAreaList();
}

function removeField(id) {
  state.activeFields.delete(id);
  renderFields();
  renderControlAreaList();
}

function clearAllFields() {
  state.activeFields.clear();
  renderFields();
  renderControlAreaList();
  document.getElementById('resultsSection').classList.add('hidden');
}

function renderFields() {
  const container = document.getElementById('fieldsContainer');
  const actions   = document.getElementById('analyseActions');
  const badge     = document.getElementById('fieldCountBadge');

  const count = state.activeFields.size;
  badge.textContent = `${count} field${count !== 1 ? 's' : ''}`;

  if (count === 0) {
    // Render the empty state inline — never reference a shared DOM node
    // that gets destroyed when we later set container.innerHTML
    container.innerHTML = `
      <div class="empty-state" id="fieldsEmptyState">
        <svg width="36" height="36" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="3" width="18" height="18" rx="2" ry="2"/><line x1="9" y1="9" x2="15" y2="9"/><line x1="9" y1="12" x2="15" y2="12"/><line x1="9" y1="15" x2="12" y2="15"/></svg>
        <p>Select control areas from the left panel to begin</p>
      </div>`;
    actions.style.display = 'none';
    return;
  }

  actions.style.display = 'flex';

  container.innerHTML = [...state.activeFields.entries()].reverse().map(([id, {area}]) => `
    <div class="field-card" id="field-card-${id}">
      <div class="field-card-header">
        <div class="field-area-name">
          <div class="field-area-icon">
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="white" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="3" width="18" height="18" rx="2"/><line x1="9" y1="9" x2="15" y2="9"/><line x1="9" y1="12" x2="15" y2="12"/><line x1="9" y1="15" x2="12" y2="15"/></svg>
          </div>
          ${escHtml(area.name)}
        </div>
        <button class="btn btn-sm btn-danger" onclick="removeField('${id}')">
          <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>
          Remove
        </button>
      </div>
      <label class="form-label" for="desc-${id}">${escHtml(area.label)}</label>
      <textarea class="form-textarea" id="desc-${id}" placeholder="${escHtml(area.placeholder)}"
        maxlength="5000" oninput="updateDescription('${id}', this.value)" rows="4"
      >${escHtml(state.activeFields.get(id)?.description || '')}</textarea>
      <div class="char-counter" id="counter-${id}">0 / 5000</div>
    </div>
  `).join('');

  // Restore char counter display for all visible textareas
  state.activeFields.forEach((field, id) => {
    updateCharCounter(id, field.description.length);
  });
}

function updateDescription(id, value) {
  if (state.activeFields.has(id)) {
    state.activeFields.get(id).description = value;
    updateCharCounter(id, value.length);
  }
}

function updateCharCounter(id, len) {
  const counter = document.getElementById(`counter-${id}`);
  if (!counter) return;
  counter.textContent = `${len} / 5000`;
  counter.className = 'char-counter' + (len > 4500 ? ' error' : len > 4000 ? ' warning' : '');
}

// ─── Analysis ─────────────────────────────────────────────────────────────────
async function runAnalysis() {
  if (!state.activeCollection) {
    showToast('No collection selected. Please select or create a collection first.', 'warning');
    return;
  }
  if (state.activeFields.size === 0) {
    showToast('Add at least one control area field first.', 'warning');
    return;
  }

  const empty = [...state.activeFields.entries()].filter(([, {description}]) => !description.trim());
  if (empty.length > 0) {
    showToast(`Please fill in descriptions for all fields (${empty.length} empty).`, 'warning');
    empty.forEach(([id]) => {
      const ta = document.getElementById(`desc-${id}`);
      if (ta) { ta.style.borderColor = 'var(--red)'; setTimeout(() => ta.style.borderColor = '', 3000); }
    });
    return;
  }

  const fields = [...state.activeFields.entries()].map(([id, {area, description}]) => ({
    control_area_id:   id,
    control_area_name: area.name,
    description:       description.trim(),
  }));

  const btn = document.getElementById('analyseBtn');
  btn.disabled = true;
  showLoading('Running compliance analysis…', `Analysing ${fields.length} control area(s)`);

  const section    = document.getElementById('resultsSection');
  const container  = document.getElementById('resultsContainer');
  const summaryEl  = document.getElementById('resultsSummary');

  section.classList.remove('hidden');
  summaryEl.innerHTML = '<span class="summary-pill pill-partial">Processing...</span>';
  
  container.innerHTML = fields.map(f => `
    <div class="result-card error-card" id="processing-${f.control_area_id}">
      <div class="result-header">
        <div class="compliance-badge badge-error">Pending</div>
        <span class="result-area-name">${escHtml(f.control_area_name)}</span>
      </div>
    </div>
  `).join('');

  section.scrollIntoView({ behavior: 'smooth', block: 'start' });

  const allResults = [];
  try {
    for (let i = 0; i < fields.length; i++) {
        const f = fields[i];
        document.getElementById('loadingSub').textContent = `Processing area ${i+1}/${fields.length}: ${f.control_area_name} (Retrieving policy & analysing...)`;
        
        const card = document.getElementById(`processing-${f.control_area_id}`);
        if(card) {
             card.querySelector('.compliance-badge').textContent = 'Retrieving & Analysing...';
             card.querySelector('.compliance-badge').className = 'compliance-badge badge-partial';
        }

        const data = await apiFetch('/analyse', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ fields: [f], collection_name: state.activeCollection }),
        });
        
        const result = data.results[0];
        allResults.push(result);
        
        const resultHTML = renderResultCard(result, i);
        const tempDiv = document.createElement('div');
        tempDiv.innerHTML = resultHTML;
        card.replaceWith(tempDiv.firstElementChild);
        toggleResult(`result-${i}`);
    }
    
    updateResultsSummary(allResults);
    showToast('Analysis complete!', 'success');
  } catch (err) {
    showToast(`Analysis failed: ${err.message}`, 'error');
  } finally {
    hideLoading();
    btn.disabled = false;
  }
}

function updateResultsSummary(results) {
  const summaryEl = document.getElementById('resultsSummary');
  const counts = { compliant: 0, partial: 0, gap: 0, injection: 0 };
  results.forEach(r => {
    if (r.injection_detected)              counts.injection++;
    else if (r.status === 'Compliant')             counts.compliant++;
    else if (r.status === 'Partially Implemented') counts.partial++;
    else                                           counts.gap++;
  });

  summaryEl.innerHTML = [
    counts.compliant > 0 ? `<span class="summary-pill pill-compliant">✓ ${counts.compliant} Compliant</span>` : '',
    counts.partial   > 0 ? `<span class="summary-pill pill-partial">~ ${counts.partial} Partially Implemented</span>` : '',
    counts.gap       > 0 ? `<span class="summary-pill pill-noncompliant">✗ ${counts.gap} Gap Identified</span>` : '',
    counts.injection > 0 ? `<span class="summary-pill pill-injection">⚠ ${counts.injection} Injection Blocked</span>` : '',
  ].join('');
}

// ─── Render results ───────────────────────────────────────────────────────────
function renderResults(results) {
  const section    = document.getElementById('resultsSection');
  const container  = document.getElementById('resultsContainer');
  
  section.classList.remove('hidden');
  section.scrollIntoView({ behavior: 'smooth', block: 'start' });

  updateResultsSummary(results);

  container.innerHTML = results.map((r, i) => renderResultCard(r, i)).join('');
  if (results.length > 0) toggleResult('result-0');
}

function renderResultCard(r, index) {
  const id = `result-${index}`;
  let cardClass, badgeClass, badgeLabel;

  if (r.injection_detected) {
    cardClass = 'injection'; badgeClass = 'badge-injection'; badgeLabel = '⚠ Injection Blocked';
  } else if (r.status === 'Compliant') {
    cardClass = 'compliant'; badgeClass = 'badge-compliant'; badgeLabel = '✓ Compliant';
  } else if (r.status === 'Partially Implemented') {
    cardClass = 'partial';   badgeClass = 'badge-partial';   badgeLabel = '~ Partially Implemented';
  } else {
    cardClass = 'noncompliant'; badgeClass = 'badge-noncompliant'; badgeLabel = '✗ Gap Identified';
  }

  let bodyContent;
  if (r.injection_detected) {
    bodyContent = `
      <div class="injection-banner">
        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>
        <div><strong>Prompt Injection Detected</strong><br>
          ${escHtml(r.summary || 'This field was blocked from AI processing.')}
          ${r.gap_detail ? `<br><br>${escHtml(r.gap_detail)}` : ''}
        </div>
      </div>`;
  } else {
    const summaryHtml = `
      <div class="result-field-block">
        <div class="result-section-title">Status</div>
        <p class="result-summary-text">${escHtml(r.status || 'Unknown')}</p>
      </div>
      <div class="result-field-block">
        <div class="result-section-title">Summary of User's Description</div>
        <p class="result-summary-text">${escHtml(r.summary || 'No summary available.')}</p>
      </div>`;

    let dynamicGapTitle = "Gap Found";
    if (r.status === 'Partially Implemented') {
        dynamicGapTitle = "Partial Implementation";
    } else if (r.status === 'Compliant') {
        dynamicGapTitle = "Compliant";
    }

    const gapHtml = r.gap_detail ? `
      <div class="result-field-block result-gap-block">
        <div class="result-section-title gap-title">
          <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>
          ${dynamicGapTitle}
        </div>
        <p class="result-gap-text">${escHtml(r.gap_detail)}</p>
      </div>` : '';

    const refHtml = r.policy_reference && r.policy_reference.length > 0 ? `
      <div class="result-field-block">
        <div class="result-section-title">
          <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/><line x1="16" y1="13" x2="8" y2="13"/><line x1="16" y1="17" x2="8" y2="17"/><polyline points="10 9 9 9 8 9"/></svg>
          Policy Reference
        </div>
        <ul class="result-list sections">
          ${r.policy_reference.map(ref => `<li>${escHtml(ref)}</li>`).join('')}
        </ul>
      </div>` : '';

    const errorHtml = r.error ? `<p class="result-error-note">Technical note: ${escHtml(r.error)}</p>` : '';
    bodyContent = summaryHtml + gapHtml + refHtml + errorHtml;
  }

  return `
    <div class="result-card ${cardClass}" id="${id}-card">
      <div class="result-header" id="${id}-header" onclick="toggleResult('${id}')">
        <div class="compliance-badge ${badgeClass}">${badgeLabel}</div>
        <span class="result-area-name">${escHtml(r.control_area_name)}</span>
        <svg class="result-chevron" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="6 9 12 15 18 9"/></svg>
      </div>
      <div class="result-body" id="${id}-body">${bodyContent}</div>
    </div>`;
}

function toggleResult(id) {
  const header = document.getElementById(`${id}-header`);
  const body   = document.getElementById(`${id}-body`);
  if (!header || !body) return;
  const isExpanded = body.classList.contains('expanded');
  body.classList.toggle('expanded', !isExpanded);
  header.classList.toggle('expanded', !isExpanded);
}

// ─── Utilities ────────────────────────────────────────────────────────────────
function toggleCard(containerId, headerEl) {
  const container = document.getElementById(containerId);
  if (!container) return;
  const chevron = headerEl.querySelector('.chevron-icon');
  if (container.style.display === 'none') {
    container.style.display = 'block';
    headerEl.classList.remove('collapsed');
    if (chevron) chevron.style.transform = 'rotate(0deg)';
  } else {
    container.style.display = 'none';
    headerEl.classList.add('collapsed');
    if (chevron) chevron.style.transform = 'rotate(180deg)';
  }
}

async function updateTotalDocsCount() {
  const el = document.getElementById('totalDocsCount');
  if (!el) return;
  if (!state.activeCollection) {
    el.textContent = '0';
    return;
  }
  try {
    const data = await apiFetch(`/documents?collection_name=${encodeURIComponent(state.activeCollection)}`);
    el.textContent = data.documents.length;
  } catch (e) {
    el.textContent = '0';
  }
}

function escHtml(str) {
  if (!str) return '';
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

// ══════════════════════════════════════════════════════════════════════════════
//  COLLECTION DELETE
// ══════════════════════════════════════════════════════════════════════════════

async function deleteActiveCollection() {
  if (!state.activeCollection) {
    showToast('No collection selected.', 'warning');
    return;
  }
  if (!confirm(`Delete collection "${state.activeCollection}" and ALL its data?\n\nThis cannot be undone.`)) return;

  try {
    await apiFetch(`/collections/${encodeURIComponent(state.activeCollection)}`, { method: 'DELETE' });
    showToast(`Collection "${state.activeCollection}" deleted.`, 'success');
    state.activeCollection = '';
    await loadCollections();
  } catch (err) {
    showToast(`Delete failed: ${err.message}`, 'error');
  }
}

// ══════════════════════════════════════════════════════════════════════════════
//  BRD ANALYSIS TAB
// ══════════════════════════════════════════════════════════════════════════════

const brdState = {
  selectedFiles: [],
};

// ─── BRD File handling ────────────────────────────────────────────────────────

function handleBrdDragOver(e) {
  e.preventDefault();
  document.getElementById('brdUploadZone').classList.add('drag-over');
}

function handleBrdDragLeave(e) {
  if (!e.currentTarget.contains(e.relatedTarget)) {
    document.getElementById('brdUploadZone').classList.remove('drag-over');
  }
}

function handleBrdDrop(e) {
  e.preventDefault();
  document.getElementById('brdUploadZone').classList.remove('drag-over');
  const files = Array.from(e.dataTransfer.files).filter(f => f.name.toLowerCase().endsWith('.pdf'));
  if (files.length === 0) { showToast('Only PDF files are supported.', 'warning'); return; }
  addBrdFilesToQueue(files);
}

function handleBrdFileSelect(e) {
  addBrdFilesToQueue(Array.from(e.target.files));
  e.target.value = '';
}

function addBrdFilesToQueue(files) {
  const existing = new Set(brdState.selectedFiles.map(f => f.name));
  brdState.selectedFiles.push(...files.filter(f => !existing.has(f.name)));
  renderBrdFileQueue();
  document.getElementById('brdStoreBtn').disabled = brdState.selectedFiles.length === 0;
}

function removeBrdFromQueue(index) {
  brdState.selectedFiles.splice(index, 1);
  renderBrdFileQueue();
  document.getElementById('brdStoreBtn').disabled = brdState.selectedFiles.length === 0;
}

function clearBrdQueue() {
  brdState.selectedFiles = [];
  renderBrdFileQueue();
  document.getElementById('brdStoreBtn').disabled = true;
}

function renderBrdFileQueue() {
  const container = document.getElementById('brdFileQueue');
  if (brdState.selectedFiles.length === 0) { container.classList.add('hidden'); return; }
  container.classList.remove('hidden');
  container.innerHTML = brdState.selectedFiles.map((f, i) => `
    <div class="file-item" id="brd-file-item-${i}">
      <div class="file-icon">
        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/></svg>
      </div>
      <span class="file-name" title="${escHtml(f.name)}">${escHtml(f.name)}</span>
      <span class="file-size">${formatBytes(f.size)}</span>
      <span class="file-status queued" id="brd-file-status-${i}">Queued</span>
      <button class="file-remove" onclick="removeBrdFromQueue(${i})" title="Remove">
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>
      </button>
    </div>
  `).join('');
}

// ─── BRD Store ────────────────────────────────────────────────────────────────

async function storeBrdDocuments() {
  if (!state.activeCollection) {
    showToast('No collection selected. Please create or select a collection first.', 'warning');
    return;
  }
  if (brdState.selectedFiles.length === 0) return;

  const btn = document.getElementById('brdStoreBtn');
  btn.disabled = true;
  showLoading(
    `Ingesting BRD into "${state.activeCollection}"…`,
    'Parsing, embedding, and storing BRD chunks.'
  );

  const formData = new FormData();
  brdState.selectedFiles.forEach(f => formData.append('files', f));
  formData.append('collection_name', state.activeCollection);

  try {
    const results = await apiFetchRaw('/ingest-brd', { method: 'POST', body: formData });

    results.forEach((r, i) => {
      const statusEl = document.getElementById(`brd-file-status-${i}`);
      if (statusEl) {
        statusEl.className = `file-status ${r.status}`;
        statusEl.textContent = r.status === 'success' ? `✓ ${r.chunks_stored} chunks` : `✗ ${r.message}`;
      }
    });

    const succeeded = results.filter(r => r.status === 'success').length;
    const failed    = results.filter(r => r.status !== 'success').length;
    if (succeeded > 0) showToast(`${succeeded} BRD document(s) stored in "${state.activeCollection}".`, 'success');
    if (failed > 0)    showToast(`${failed} BRD document(s) failed to ingest.`, 'error');

    const failedNames = new Set(results.filter(r => r.status !== 'success').map(r => r.filename));
    brdState.selectedFiles = brdState.selectedFiles.filter(f => failedNames.has(f.name));
    renderBrdFileQueue();
    await loadBrdDocuments();
    updateTotalDocsCount();
    loadClassifiedChunks('brd');
  } catch (err) {
    showToast(`BRD ingestion failed: ${err.message}`, 'error');
  } finally {
    hideLoading();
    btn.disabled = brdState.selectedFiles.length === 0;
  }
}

// ─── BRD Analysis ─────────────────────────────────────────────────────────────

async function runBrdAnalysis() {
  if (!state.activeCollection) {
    showToast('No collection selected. Please select or create a collection first.', 'warning');
    return;
  }

  const btn = document.getElementById('brdAnalyseBtn');
  btn.disabled = true;
  showLoading(
    'Running BRD gap analysis…',
    'Classifying chunks and analysing 7 control areas sequentially.'
  );

  const section   = document.getElementById('brdResultsSection');
  const container = document.getElementById('brdResultsContainer');
  const summaryEl = document.getElementById('brdResultsSummary');
  const classEl   = document.getElementById('brdClassificationSummary');

  section.classList.remove('hidden');
  summaryEl.innerHTML = '<span class="summary-pill pill-partial">Processing…</span>';
  container.innerHTML = '<div class="brd-processing-msg">Classifying and analysing… This may take several minutes.</div>';
  classEl.classList.add('hidden');

  section.scrollIntoView({ behavior: 'smooth', block: 'start' });

  const selectedAreas = Array.from(state.activeFields.keys());

  try {
    const data = await apiFetch('/analyse-brd', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        collection_name: state.activeCollection,
        parallel: false,
        active_areas: selectedAreas.length > 0 ? selectedAreas : undefined
      }),
    });

    // Render classification summary
    if (data.classification && data.classification.length > 0) {
      renderClassificationSummary(data.classification);
    }

    // Render results
    renderBrdResults(data.results);
    showToast('BRD analysis complete!', 'success');

  } catch (err) {
    showToast(`BRD analysis failed: ${err.message}`, 'error');
    container.innerHTML = `<div class="empty-state"><p style="color:var(--red)">${escHtml(err.message)}</p></div>`;
  } finally {
    hideLoading();
    btn.disabled = false;
  }
}

function renderClassificationSummary(classification) {
  const el = document.getElementById('brdClassificationSummary');
  el.classList.remove('hidden');
  el.innerHTML = `
    <div class="classification-card">
      <h3 class="classification-title">
        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="3" width="7" height="7"/><rect x="14" y="3" width="7" height="7"/><rect x="14" y="14" width="7" height="7"/><rect x="3" y="14" width="7" height="7"/></svg>
        Chunk Classification Summary
      </h3>
      <table class="classification-table">
        <thead>
          <tr>
            <th>Control Area</th>
            <th>Policy Chunks</th>
            <th>BRD Chunks</th>
          </tr>
        </thead>
        <tbody>
          ${classification.map(c => `
            <tr>
              <td>${escHtml(c.control_area_name)}</td>
              <td class="chunk-count">${c.policy_chunk_count}</td>
              <td class="chunk-count ${c.brd_chunk_count === 0 ? 'zero-count' : ''}">${c.brd_chunk_count}</td>
            </tr>
          `).join('')}
        </tbody>
      </table>
    </div>`;
}

function renderBrdResults(results) {
  const container = document.getElementById('brdResultsContainer');
  const summaryEl = document.getElementById('brdResultsSummary');

  // Summary pills
  const counts = { compliant: 0, partial: 0, gap: 0 };
  results.forEach(r => {
    if (r.status === 'Compliant')             counts.compliant++;
    else if (r.status === 'Partially Implemented') counts.partial++;
    else                                           counts.gap++;
  });

  summaryEl.innerHTML = [
    counts.compliant > 0 ? `<span class="summary-pill pill-compliant">✓ ${counts.compliant} Compliant</span>` : '',
    counts.partial   > 0 ? `<span class="summary-pill pill-partial">~ ${counts.partial} Partial</span>` : '',
    counts.gap       > 0 ? `<span class="summary-pill pill-noncompliant">✗ ${counts.gap} Gap</span>` : '',
  ].join('');

  container.innerHTML = results.map((r, i) => renderBrdResultCard(r, i)).join('');
  if (results.length > 0) toggleBrdResult('brd-result-0');
}

function renderBrdResultCard(r, index) {
  const id = `brd-result-${index}`;
  let cardClass, badgeClass, badgeLabel;

  if (r.status === 'Compliant') {
    cardClass = 'compliant'; badgeClass = 'badge-compliant'; badgeLabel = '✓ Compliant';
  } else if (r.status === 'Partially Implemented') {
    cardClass = 'partial'; badgeClass = 'badge-partial'; badgeLabel = '~ Partially Implemented';
  } else {
    cardClass = 'noncompliant'; badgeClass = 'badge-noncompliant'; badgeLabel = '✗ Gap Identified';
  }

  const chunkInfo = `<div class="brd-chunk-info">
    <span class="chunk-badge policy-badge">Policy: ${r.policy_chunk_count} chunks</span>
    <span class="chunk-badge brd-badge">BRD: ${r.brd_chunk_count} chunks</span>
  </div>`;

  const summaryHtml = `
    <div class="result-field-block">
      <div class="result-section-title">Summary</div>
      <p class="result-summary-text">${escHtml(r.summary || 'No summary available.')}</p>
    </div>`;

  let dynamicGapTitle = 'Gap Found';
  if (r.status === 'Partially Implemented') dynamicGapTitle = 'Partial Implementation';
  else if (r.status === 'Compliant') dynamicGapTitle = 'Compliant';

  const gapHtml = r.gap_detail ? `
    <div class="result-field-block result-gap-block">
      <div class="result-section-title gap-title">
        <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>
        ${dynamicGapTitle}
      </div>
      <p class="result-gap-text">${escHtml(r.gap_detail)}</p>
    </div>` : '';

  const policyRefHtml = r.policy_references && r.policy_references.length > 0 ? `
    <div class="result-field-block">
      <div class="result-section-title">
        <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/><line x1="16" y1="13" x2="8" y2="13"/><line x1="16" y1="17" x2="8" y2="17"/></svg>
        Policy References
      </div>
      <ul class="result-list sections">
        ${r.policy_references.map(ref => `<li>${escHtml(ref)}</li>`).join('')}
      </ul>
    </div>` : '';

  const brdRefHtml = r.brd_references && r.brd_references.length > 0 ? `
    <div class="result-field-block">
      <div class="result-section-title">
        <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/></svg>
        BRD References
      </div>
      <ul class="result-list sections brd-ref-list">
        ${r.brd_references.map(ref => `<li>${escHtml(ref)}</li>`).join('')}
      </ul>
    </div>` : '';

  const errorHtml = r.error ? `<p class="result-error-note">Technical note: ${escHtml(r.error)}</p>` : '';
  const bodyContent = chunkInfo + summaryHtml + gapHtml + policyRefHtml + brdRefHtml + errorHtml;

  return `
    <div class="result-card ${cardClass}" id="${id}-card">
      <div class="result-header" id="${id}-header" onclick="toggleBrdResult('${id}')">
        <div class="compliance-badge ${badgeClass}">${badgeLabel}</div>
        <span class="result-area-name">${escHtml(r.control_area_name)}</span>
        <svg class="result-chevron" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="6 9 12 15 18 9"/></svg>
      </div>
      <div class="result-body" id="${id}-body">${bodyContent}</div>
    </div>`;
}

function toggleBrdResult(id) {
  const header = document.getElementById(`${id}-header`);
  const body   = document.getElementById(`${id}-body`);
  if (!header || !body) return;
  const isExpanded = body.classList.contains('expanded');
  body.classList.toggle('expanded', !isExpanded);
  header.classList.toggle('expanded', !isExpanded);
}

// ══════════════════════════════════════════════════════════════════════════════
//  CLASSIFIED CHUNKS (human-in-the-loop)
// ══════════════════════════════════════════════════════════════════════════════

function getControlAreaIds() {
  return state.controlAreas.map(ca => ca.id).concat(['noise']);
}

function getControlAreaName(id) {
  if (id === 'noise' || id === 'unclassified') return 'Noise / Unclassified';
  const ca = state.controlAreas.find(c => c.id === id);
  return ca ? ca.name : String(id);
}

async function loadClassifiedChunks(docType) {
  if (!state.activeCollection) return;
  const containerID = docType === 'policy' ? 'policyChunksContainer' : 'brdChunksContainer';
  const cardID = docType === 'policy' ? 'policyChunksCard' : 'brdChunksCard';
  const container = document.getElementById(containerID);
  const card = document.getElementById(cardID);

  try {
    const data = await apiFetch(
      `/classified-chunks?collection_name=${encodeURIComponent(state.activeCollection)}&doc_type=${docType}`
    );

    if (!data.chunks || data.chunks.length === 0) {
      card.style.display = 'none';
      return;
    }

    card.style.display = 'block';
    renderClassifiedChunks(container, data.chunks, data.control_area_summary, docType);
  } catch (err) {
    card.style.display = 'block';
    container.innerHTML = `<div class="empty-state"><p style="color:var(--red)">Failed to load chunks: ${err.message}</p></div>`;
  }
}

function renderClassifiedChunks(container, chunks, summary, docType) {
  // Group chunks by first control area
  const groups = {};
  getControlAreaIds().forEach(id => { groups[id] = []; });

  chunks.forEach(c => {
    if (c.control_areas && c.control_areas.length > 0) {
      const primary = c.control_areas[0];
      if (groups[primary]) {
        groups[primary].push(c);
      } else {
        groups['noise'].push(c);
      }
    } else {
      groups['noise'].push(c);
    }
  });

  let html = '<div class="chunks-accordion">';

  // Render each control area section
  getControlAreaIds().forEach(areaId => {
    // If we dynamically added a custom area during the session, it might not have an array initialized if done after load classified
    const areaChunks = groups[areaId] || [];
    const count = areaChunks.length;
    const areaName = getControlAreaName(areaId);
    const isExpanded = false; // always start collapsed

    html += `
      <div class="chunk-area-section">
        <div class="chunk-area-header ${isExpanded ? 'expanded' : ''}" onclick="toggleChunkArea(this)">
          <span class="chunk-area-name">${escHtml(areaName)}</span>
          <span class="chunk-area-count ${count === 0 ? 'zero' : ''}">${count}</span>
          <svg class="chunk-area-chevron" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="6 9 12 15 18 9"/></svg>
        </div>
        <div class="chunk-area-body ${isExpanded ? 'expanded' : ''}">
          ${areaChunks.length === 0
            ? '<div class="empty-state" style="padding:16px"><p>No chunks classified here</p></div>'
            : areaChunks.map(c => renderChunkCard(c, docType)).join('')
          }
        </div>
      </div>`;
  });

  html += '</div>';
  container.innerHTML = html;
}

function _extractBodyText(rawText) {
  // Strip the context prefix: "[file | Page N | Section]\nHeading\n\n" added by pdf_parser.
  // The prefix ends at the first double-newline after the bracket line.
  const doubleNl = rawText.indexOf('\n\n');
  if (doubleNl !== -1 && rawText.startsWith('[')) {
    return rawText.substring(doubleNl + 2).trim();
  }
  return rawText.trim();
}

function renderChunkCard(chunk, docType) {
  const uuid = chunk.uuid;
  const rawText = chunk.text || '';
  const bodyText = _extractBodyText(rawText);           // clean body, no metadata
  const heading = chunk.heading || '';
  const preview = bodyText.length > 160 ? bodyText.substring(0, 160) + '…' : bodyText;
  const conf = chunk.confidence || 0;
  const confClass = conf >= 1.0 ? 'conf-verified' : conf >= 0.5 ? 'conf-high' : conf >= 0.3 ? 'conf-medium' : 'conf-low';
  const confLabel = conf >= 1.0 ? 'Verified' : `${(conf * 100).toFixed(0)}%`;
  const currentAreas = chunk.control_areas || [];
  // Escape for use inside a data attribute (double-quotes must be entity-encoded)
  const fullTextAttr = bodyText.replace(/&/g,'&amp;').replace(/"/g,'&quot;');

  const optionsHtml = getControlAreaIds().filter(id => id !== 'noise').map(id =>
    `<option value="${id}" ${currentAreas.includes(id) ? 'selected' : ''}>${escHtml(getControlAreaName(id))}</option>`
  ).join('');

  return `
    <div class="chunk-card" id="chunk-${uuid}">
      <div class="chunk-card-top">
        ${heading ? `<span class="chunk-heading">${escHtml(heading)}</span>` : ''}
        <span class="conf-badge ${confClass}">${confLabel}</span>
      </div>
      <p class="chunk-preview"
         id="chunk-text-${uuid}"
         data-full-text="${fullTextAttr}"
         data-expanded="false"
         onclick="toggleChunkText(this)"
         title="Click to expand full text">${escHtml(preview)}</p>
      <div class="chunk-card-actions">
        <select class="chunk-area-select" id="chunk-select-${uuid}"
                onchange="updateChunkClassification('${uuid}', this.value)"
                title="Change control area">
          <option value="">— Unclassified —</option>
          ${optionsHtml}
        </select>
        <span class="chunk-source">${escHtml(chunk.source_file || '')}</span>
      </div>
    </div>`;
}

function toggleChunkText(el) {
  const fullText = el.dataset.fullText || '';
  if (el.dataset.expanded === 'true') {
    // Collapse back to preview
    el.textContent = fullText.length > 160 ? fullText.substring(0, 160) + '…' : fullText;
    el.dataset.expanded = 'false';
    el.title = 'Click to expand full text';
  } else {
    // Expand to full clean body text
    el.textContent = fullText || '(no text)';
    el.dataset.expanded = 'true';
    el.title = 'Click to collapse';
  }
}

function toggleChunkArea(header) {
  const body = header.nextElementSibling;
  const isExpanded = body.classList.contains('expanded');
  body.classList.toggle('expanded', !isExpanded);
  header.classList.toggle('expanded', !isExpanded);
}

async function updateChunkClassification(uuid, newAreaId) {
  if (!state.activeCollection) return;

  const controlAreas = newAreaId ? [newAreaId] : [];

  try {
    await apiFetch(`/chunks/${uuid}/classification`, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        collection_name: state.activeCollection,
        control_areas: controlAreas,
      }),
    });

    showToast(`Chunk reassigned to ${newAreaId ? getControlAreaName(newAreaId) : 'Unclassified'}.`, 'success', 2000);

    // Determine which doc type this chunk belongs to and reload
    const card = document.getElementById(`chunk-${uuid}`);
    if (card) {
      const isPolicy = document.getElementById('policyChunksContainer')?.contains(card);
      loadClassifiedChunks(isPolicy ? 'policy' : 'brd');
    }
  } catch (err) {
    showToast(`Failed to update: ${err.message}`, 'error');
  }
}
