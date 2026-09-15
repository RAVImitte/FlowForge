/**
 * FlowForge API Client
 * Wraps all REST API calls with error handling and ETag management.
 */
const FlowForgeAPI = (function () {
    const API_BASE = '/api/v1';

    /**
     * Extract error message from a fetch response.
     */
    async function extractError(response) {
        let detail = `HTTP ${response.status}`;
        try {
            const body = await response.json();
            detail = body.detail || body.title || body.message || detail;
            if (body.violations && body.violations.length > 0) {
                detail += ': ' + body.violations.join('; ');
            }
        } catch (_) {
            /* response had no JSON body */
        }
        return detail;
    }

    /**
     * Parse the ETag header value (e.g. '"0"' -> 0).
     */
    function parseEtag(etagHeader) {
        if (!etagHeader) return null;
        const match = etagHeader.match(/"(\d+)"/);
        return match ? parseInt(match[1], 10) : null;
    }

    /**
     * Generic request wrapper.
     */
    async function request(method, path, { body, headers, parseEtagHeader } = {}) {
        const opts = {
            method,
            headers: {
                'Content-Type': 'application/json',
                ...(headers || {}),
            },
        };
        if (body !== undefined) {
            opts.body = JSON.stringify(body);
        }

        const response = await fetch(`${API_BASE}${path}`, opts);

        if (!response.ok) {
            const error = await extractError(response);
            throw new Error(error);
        }

        const result = { data: null, etag: null };

        if (response.status !== 204) {
            result.data = await response.json();
        }
        if (parseEtagHeader) {
            result.etag = parseEtag(response.headers.get('ETag'));
        }
        return result;
    }

    /* ---- Workflow endpoints ---- */

    async function listWorkflows(page = 0, size = 20) {
        const res = await request('GET', `/workflows?page=${page}&size=${size}`);
        return res.data;
    }

    async function getWorkflow(id) {
        const res = await request('GET', `/workflows/${id}`, { parseEtagHeader: true });
        return res;
    }

    async function createWorkflow(workflowData) {
        const res = await request('POST', '/workflows', {
            body: workflowData,
            parseEtagHeader: true,
        });
        return res;
    }

    async function updateWorkflow(id, lockVersion, workflowData) {
        const res = await request('PUT', `/workflows/${id}`, {
            body: workflowData,
            headers: { 'If-Match': `"${lockVersion}"` },
            parseEtagHeader: true,
        });
        return res;
    }

    async function publishWorkflow(id, lockVersion) {
        const res = await request('POST', `/workflows/${id}/publish`, {
            headers: { 'If-Match': `"${lockVersion}"` },
            parseEtagHeader: true,
        });
        return res;
    }

    async function archiveWorkflow(id, lockVersion) {
        await request('DELETE', `/workflows/${id}`, {
            headers: { 'If-Match': `"${lockVersion}"` },
        });
    }

    /* ---- Execution endpoints ---- */

    async function startExecution(workflowId, idempotencyKey) {
        const res = await request('POST', `/workflows/${workflowId}/executions`, {
            headers: { 'Idempotency-Key': idempotencyKey },
        });
        return res.data;
    }

    async function listExecutions(page = 0, size = 20, status = null) {
        let path = `/executions?page=${page}&size=${size}`;
        if (status && status !== 'ALL') path += `&status=${status}`;
        const res = await request('GET', path);
        return res.data;
    }

    async function getExecution(executionId) {
        const res = await request('GET', `/executions/${executionId}`);
        return res.data;
    }

    async function cancelExecution(executionId) {
        const res = await request('POST', `/executions/${executionId}/cancel`);
        return res.data;
    }

    return {
        listWorkflows,
        getWorkflow,
        createWorkflow,
        updateWorkflow,
        publishWorkflow,
        archiveWorkflow,
        startExecution,
        listExecutions,
        getExecution,
        cancelExecution,
    };
})();

