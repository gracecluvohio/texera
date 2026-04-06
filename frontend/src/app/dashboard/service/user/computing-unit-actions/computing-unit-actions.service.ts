import { Injectable } from "@angular/core";
import { Observable } from "rxjs";
import { NzModalService } from "ng-zorro-antd/modal";
import { ShareAccessComponent } from "../../../component/user/share-access/share-access.component";
import { WorkflowComputingUnitManagingService } from "../../../../workspace/service/workflow-computing-unit/workflow-computing-unit-managing.service";
import {
  DashboardWorkflowComputingUnit,
  WorkflowComputingUnitType,
} from "../../../../workspace/types/workflow-computing-unit";
import { NotificationService } from "../../../../common/service/notification/notification.service";
import { unitTypeMessageTemplate } from "../../../../common/util/computing-unit.util";
import { ComputingUnitStatusService } from "../../../../workspace/service/computing-unit-status/computing-unit-status.service";
import { extractErrorMessage } from "../../../../common/util/error";

export interface StartComputingUnitRequest {
  type: WorkflowComputingUnitType;
  name: string;
  cpu: string;
  memory: string;
  gpu: string;
  jvmMemorySize: string;
  shmSize: string;
  localUri: string;
}

@Injectable({
  providedIn: "root",
})
export class ComputingUnitActionsService {
  constructor(
    private modalService: NzModalService,
    private computingUnitService: WorkflowComputingUnitManagingService,
    private notificationService: NotificationService,
    private computingUnitStatusService: ComputingUnitStatusService
  ) {}

  openShareAccessModal(cuid: number, inWorkspace: boolean = true): void {
    this.modalService.create({
      nzContent: ShareAccessComponent,
      nzData: {
        writeAccess: true,
        type: "computing-unit",
        id: cuid,
        inWorkspace,
      },
      nzFooter: null,
      nzTitle: "Share this computing unit with others",
      nzCentered: true,
      nzWidth: "800px",
    });
  }

  create(request: StartComputingUnitRequest): Observable<DashboardWorkflowComputingUnit> {
    if (request.type === "kubernetes") {
      return this.computingUnitService.createKubernetesBasedComputingUnit(
        request.name,
        request.cpu,
        request.memory,
        request.gpu,
        request.jvmMemorySize,
        request.shmSize
      );
    }

    if (request.type === "local") {
      return this.computingUnitService.createLocalComputingUnit(request.name, request.localUri);
    }

    throw new Error("Unsupported computing unit type");
  }

  confirmAndTerminate(cuid: number, unit: DashboardWorkflowComputingUnit): void {
    if (!unit.computingUnit.uri) {
      this.notificationService.error("Invalid computing unit.");
      return;
    }

    const unitName = unit.computingUnit.name;
    const unitType = unit?.computingUnit.type || "kubernetes"; // fallback
    const templates = unitTypeMessageTemplate[unitType];

    // Show confirmation modal
    this.modalService.confirm({
      nzTitle: templates.terminateTitle,
      nzContent: templates.terminateWarning
        ? `
      <p>Are you sure you want to terminate <strong>${unitName}</strong>?</p>
      ${templates.terminateWarning}
    `
        : `
      <p>Are you sure you want to disconnect from <strong>${unitName}</strong>?</p>
    `,
      nzOkText: unitType === "local" ? "Disconnect" : "Terminate",
      nzOkType: "primary",
      nzOnOk: () => {
        // Use the ComputingUnitStatusService to handle termination
        // This will properly close the websocket before terminating the unit
        this.computingUnitStatusService.terminateComputingUnit(cuid).subscribe({
          next: (success: boolean) => {
            if (success) {
              this.notificationService.success(`Terminated Computing Unit: ${unitName}`);
            } else {
              this.notificationService.error("Failed to terminate computing unit");
            }
          },
          error: (err: unknown) => {
            this.notificationService.error(`Failed to terminate computing unit: ${extractErrorMessage(err)}`);
          },
        });
      },
      nzCancelText: "Cancel",
    });
  }
}
