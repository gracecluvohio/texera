import { Injectable } from "@angular/core";
import { NzModalService } from "ng-zorro-antd/modal";
import { ShareAccessComponent } from "../../../component/user/share-access/share-access.component";

@Injectable({
  providedIn: "root",
})
export class ComputingUnitActionsService {
  constructor(private modalService: NzModalService) {}

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
}
