import { Injectable } from "@angular/core";
import { Observable } from "rxjs";
import { NzModalService } from "ng-zorro-antd/modal";
import { ShareAccessComponent } from "../../../component/user/share-access/share-access.component";
import { WorkflowComputingUnitManagingService } from "../../../../workspace/service/workflow-computing-unit/workflow-computing-unit-managing.service";
import {
  DashboardWorkflowComputingUnit,
  WorkflowComputingUnitType,
} from "../../../../workspace/types/workflow-computing-unit";

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
    private computingUnitService: WorkflowComputingUnitManagingService
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
}
