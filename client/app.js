// API Configuration
const API_BASE = '/api/v1';

// DOM Elements
const dropZone = document.getElementById('drop-zone');
const fileInput = document.getElementById('file-input');
const taskList = document.getElementById('task-list');
const addProjectBtn = document.getElementById('add-project-btn');

// Stats Elements
const statsTotal = document.getElementById('stats-total');
const statsEnded = document.getElementById('stats-ended');
const statsRunning = document.getElementById('stats-running');
const statsPending = document.getElementById('stats-pending');

let activeTasks = new Set();
let allTasksData = {};

// Event Listeners
dropZone.onclick = () => fileInput.click();
if (addProjectBtn) addProjectBtn.onclick = () => fileInput.click();

fileInput.onchange = (e) => {
    if (e.target.files.length > 0) {
        handleUpload(e.target.files[0]);
    }
};

// Drag and drop logic
dropZone.ondragover = (e) => {
    e.preventDefault();
    dropZone.classList.add('drag-over');
};

dropZone.ondragleave = () => {
    dropZone.classList.remove('drag-over');
};

dropZone.ondrop = (e) => {
    e.preventDefault();
    dropZone.classList.remove('drag-over');
    if (e.dataTransfer.files.length > 0) {
        handleUpload(e.dataTransfer.files[0]);
    }
};

// API Interactions
async function handleUpload(file) {
    if (!file.name.endsWith('.zip')) {
        alert('Please upload a ZIP file');
        return;
    }

    // Visual feedback on dropzone
    const dropZoneText = dropZone.querySelector('span');
    const originalText = dropZoneText.innerText;
    dropZone.classList.add('loading');
    dropZoneText.innerText = 'Uploading ZIP...';

    // Show immediate loading placeholder in list
    const placeholderId = 'uploading-' + Date.now();
    updateTaskUI(placeholderId, {
        filename: file.name,
        status: 'processing',
        message: 'Uploading and initializing migration...'
    });

    const formData = new FormData();
    formData.append('file', file);

    try {
        const response = await fetch(`${API_BASE}/migrate`, {
            method: 'POST',
            body: formData
        });

        if (!response.ok) throw new Error('Upload failed');

        const data = await response.json();

        // Remove placeholder and start real polling
        const placeholder = document.getElementById(`task-${placeholderId}`);
        if (placeholder) placeholder.remove();

        activeTasks.add(data.task_id);
        pollTaskStatus(data.task_id);
    } catch (err) {
        console.error(err);
        const placeholder = document.getElementById(`task-${placeholderId}`);
        if (placeholder) {
            updateTaskUI(placeholderId, {
                filename: file.name,
                status: 'failed',
                message: 'Error: Upload failed'
            });
        } else {
            alert('Error starting migration');
        }
    } finally {
        // Restore dropzone
        dropZone.classList.remove('loading');
        if (dropZoneText) dropZoneText.innerText = originalText;
    }
}

async function pollTaskStatus(taskId) {
    const poll = async () => {
        try {
            const response = await fetch(`${API_BASE}/status/${taskId}`);
            const data = await response.json();

            allTasksData[taskId] = data;
            updateTaskUI(taskId, data);
            updateStats();

            if (data.status === 'processing') {
                setTimeout(poll, 800);
            } else {
                activeTasks.delete(taskId);
            }
        } catch (err) {
            console.error('Polling error:', err);
        }
    };

    poll();
}

function updateStats() {
    const tasks = Object.values(allTasksData);
    const total = tasks.length;
    const ended = tasks.filter(t => t.status === 'completed' || t.status === 'failed').length;
    const running = tasks.filter(t => t.status === 'processing').length;

    if (statsTotal) statsTotal.innerText = total;
    if (statsEnded) statsEnded.innerText = ended;
    if (statsRunning) statsRunning.innerText = running;
    if (statsPending) statsPending.innerText = total > 0 ? 0 : 0; // Simplified for now
}

function updateTaskUI(taskId, data) {
    let taskElement = document.getElementById(`task-${taskId}`);

    if (!taskElement) {
        const emptyState = taskList.querySelector('.empty-state');
        if (emptyState) emptyState.remove();

        taskElement = document.createElement('div');
        taskElement.id = `task-${taskId}`;
        taskElement.className = 'task-item-mini';
        taskList.prepend(taskElement);
    }

    const stepsHtml = data.steps ? `
        <div class="stepper">
            ${data.steps.map(step => `
                <div class="step ${step.status}">
                    <div class="step-indicator">
                        ${step.status === 'completed' ? '<i data-lucide="check"></i>' : (step.status === 'failed' ? '<i data-lucide="x"></i>' : '')}
                    </div>
                    <span class="step-label">${step.label}</span>
                </div>
            `).join('')}
        </div>
    ` : '';

    const icon = data.status === 'completed' ? 'check-circle' : (data.status === 'failed' ? 'alert-circle' : 'loader');

    taskElement.innerHTML = `
        <div class="task-main">
            <i data-lucide="${icon}" class="status-icon ${data.status}"></i>
            <div class="mini-info">
                <h4>${data.report?.course || data.filename || 'Migration Task'}</h4>
                <p class="status-msg">${data.message || 'Processing migration...'}</p>
            </div>
            <div class="status-badge badge ${data.status === 'completed' ? 'success' : (data.status === 'failed' ? 'error' : 'warning')}">
                ${data.status}
            </div>
        </div>
        
        ${data.status !== 'failed' ? `
            <div class="progress-container">
                <div class="progress-bar" style="width: ${data.progress || 0}%"></div>
                <span class="progress-val">${data.progress || 0}%</span>
            </div>
        ` : ''}

        ${stepsHtml}
        
        ${data.status === 'completed' && data.report ? `
            <div class="task-footer">
                <span class="report-id">ID: ${taskId.split('-')[0].toUpperCase()}</span>
                <button class="btn btn-sm" onclick="window.location.href='report.html?taskId=${taskId}'">View Report</button>
            </div>
        ` : ''}
    `;

    if (window.lucide) lucide.createIcons();
}

// Initial icon generation
document.addEventListener('DOMContentLoaded', () => {
    if (window.lucide) lucide.createIcons();
});
