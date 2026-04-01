import { ComponentFixture, TestBed } from "@angular/core/testing";
import { UserComputingUnitComponent } from "./user-computing-unit.component";
import { NzCardModule } from "ng-zorro-antd/card";

describe("UserComputingUnitComponent", () => {
  let component: UserComputingUnitComponent;
  let fixture: ComponentFixture<UserComputingUnitComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [UserComputingUnitComponent],
      imports: [NzCardModule],
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(UserComputingUnitComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });
});
