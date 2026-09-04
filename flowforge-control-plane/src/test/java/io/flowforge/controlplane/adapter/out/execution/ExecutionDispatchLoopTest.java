package io.flowforge.controlplane.adapter.out.execution;

import io.flowforge.application.execution.WorkflowExecutionService;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ExecutionDispatchLoopTest {
    @Test
    void claimsDurableReadyWorkWhenTheApplicationStarts() {
        WorkflowExecutionService service = mock(WorkflowExecutionService.class);
        when(service.dispatchReadyTasks(100)).thenReturn(2);
        ExecutionDispatchLoop loop = new ExecutionDispatchLoop(service);

        loop.run(null);

        verify(service).dispatchReadyTasks(100);
    }
}
