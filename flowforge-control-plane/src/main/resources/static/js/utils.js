/**
 * FlowForge UI Utilities
 * Shared helpers for toast notifications, loading spinners, and formatting.
 */
const FlowForgeUI = (function () {
    /**
     * Show a toast notification.
     * @param {string} message - The message to display.
     * @param {string} type - 'success' | 'danger' | 'warning' | 'info'
     */
    function toast(message, type = 'info') {
        let container = document.querySelector('.toast-container');
        if (!container) {
            container = document.createElement('div');
            container.className = 'toast-container';
            document.body.appendChild(container);
        }

        const bgClass = {
            success: 'text-bg-success',
            danger: 'text-bg-danger',
            warning: 'text-bg-warning',
            info: 'text-bg-primary',
        }[type] || 'text-bg-primary';

        const toastEl = document.createElement('div');
        toastEl.className = `toast align-items-center ${bgClass} border-0`;
        toastEl.role = 'alert';
        toastEl.innerHTML = `
            <div class="d-flex">
                <div class="toast-body">${escapeHtml(message)}</div>
                <button type="button" class="btn-close btn-close-white me-2 m-auto" data-bs-dismiss="toast"></button>
            </div>
        `;
        container.appendChild(toastEl);

        const toast = new bootstrap.Toast(toastEl, { delay: 5000 });
        toast.show();
        toastEl.addEventListener('hidden.bs.toast', () => toastEl.remove());
    }

    /**
     * Show / hide the full-page loading overlay.
     */
    function showLoading() {
        let overlay = document.getElementById('loading-overlay');
        if (!overlay) {
            overlay = document.createElement('div');
            overlay.id = 'loading-overlay';
            overlay.className = 'loading-overlay';
            overlay.innerHTML = '<div class="spinner-border text-primary" role="status"><span class="visually-hidden">Loading…</span></div>';
            document.body.appendChild(overlay);
        }
        overlay.style.display = 'flex';
    }

    function hideLoading() {
        const overlay = document.getElementById('loading-overlay');
        if (overlay) overlay.style.display = 'none';
    }

    /**
     * Escape HTML to prevent XSS in dynamically built strings.
     */
    function escapeHtml(text) {
        if (text === null || text === undefined) return '';
        const div = document.createElement('div');
        div.textContent = String(text);
        return div.innerHTML;
    }

    /**
     * Format an ISO timestamp into a readable local string.
     */
    function formatDateTime(isoString) {
        if (!isoString) return '—';
        const d = new Date(isoString);
        if (isNaN(d.getTime())) return isoString;
        return d.toLocaleString(undefined, {
            year: 'numeric', month: 'short', day: 'numeric',
            hour: '2-digit', minute: '2-digit',
        });
    }

    /**
     * Return a Bootstrap badge class for a workflow version status.
     */
    function versionStatusBadge(status) {
        const map = {
            DRAFT: 'badge-draft',
            PUBLISHED: 'badge-published',
        };
        return `<span class="badge ${map[status] || 'bg-secondary'}">${escapeHtml(status)}</span>`;
    }

    /**
     * Return a Bootstrap badge class for a lifecycle status.
     */
    function lifecycleStatusBadge(status) {
        const map = {
            ACTIVE: 'badge-active',
            ARCHIVED: 'badge-archived',
        };
        return `<span class="badge ${map[status] || 'bg-secondary'}">${escapeHtml(status)}</span>`;
    }

    /**
     * Return a coloured badge for a task / execution status.
     */
    function taskStatusBadge(status) {
        const map = {
            READY: 'bg-warning',
            BLOCKED: 'bg-secondary',
            RUNNING: 'bg-primary',
            SUCCEEDED: 'bg-success',
            FAILED: 'bg-danger',
            CANCELLED: 'bg-secondary',
            CANCELLING: 'bg-info',
            PENDING: 'bg-secondary',
            COMPLETED: 'bg-success',
            RETRY_SCHEDULED: 'bg-warning',
            TIMED_OUT: 'bg-danger',
        };
        return `<span class="badge ${map[status] || 'bg-secondary'}">${escapeHtml(status)}</span>`;
    }

    /**
     * Return a coloured badge for a workflow execution status.
     */
    function executionStatusBadge(status) {
        const map = {
            PENDING: 'bg-secondary',
            RUNNING: 'bg-primary',
            CANCELLING: 'bg-info',
            SUCCEEDED: 'bg-success',
            FAILED: 'bg-danger',
            CANCELLED: 'bg-secondary',
        };
        return `<span class="badge ${map[status] || 'bg-secondary'}">${escapeHtml(status)}</span>`;
    }

    /**
     * Return a coloured badge for a task attempt status.
     */
    function attemptStatusBadge(status) {
        const map = {
            RUNNING: 'bg-primary',
            SUCCEEDED: 'bg-success',
            FAILED: 'bg-danger',
            TIMED_OUT: 'bg-danger',
            CANCELLED: 'bg-secondary',
        };
        return `<span class="badge ${map[status] || 'bg-secondary'}">${escapeHtml(status)}</span>`;
    }


    /**
     * Generate a UUID v4 (used for idempotency keys).
     */
    function uuid() {
        if (crypto && crypto.randomUUID) return crypto.randomUUID();
        return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (c) => {
            const r = (Math.random() * 16) | 0;
            const v = c === 'x' ? r : (r & 0x3) | 0x8;
            return v.toString(16);
        });
    }

    return {
        toast,
        showLoading,
        hideLoading,
        escapeHtml,
        formatDateTime,
        versionStatusBadge,
        lifecycleStatusBadge,
        taskStatusBadge,
        executionStatusBadge,
        attemptStatusBadge,
        uuid,
    };
})();

