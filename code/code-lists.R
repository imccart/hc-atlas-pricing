# Meta --------------------------------------------------------------------

## Title:         Code Lists
## Author:        Ian McCarthy
## Date Created:  2026-02-16
## Description:   Central definitions of target billing codes for the pricing
##                pipeline. Sourced by downstream scripts.

# Target MS-DRGs (inpatient) -----------------------------------------------
# 10 common DRGs spanning joint/spine, OB, cardiac, GI

target_drgs <- tibble(
  code = c("470", "473", "743", "766", "767",
           "291", "292", "329", "330", "871"),
  code_type = "MS-DRG",
  label = c(
    "Major hip/knee joint replacement",
    "Cervical spinal fusion",
    "Uterine & adnexa procedures (non-malignant)",
    "Cesarean section w/o CC/MCC",
    "Vaginal delivery w/o complicating diagnoses",
    "Heart failure & shock w MCC",
    "Heart failure & shock w CC",
    "Major small & large bowel procedures w MCC",
    "Major small & large bowel procedures w CC",
    "Septicemia or severe sepsis w/o MV >96 hrs w MCC"
  )
)

# Target CPT/HCPCS (outpatient) --------------------------------------------
# 23 common codes: ED, office visits, imaging, cardiac, GI, ortho, labs

target_cpts <- tibble(
  code = c("99213", "99214", "99215",
           "99281", "99282", "99283", "99284", "99285",
           "70553", "74177",
           "27447", "29881", "43239",
           "93000", "93306", "93452",
           "36415", "80053", "85025",
           "71046", "72148",
           "G0105", "G0121"),
  code_type = "CPT/HCPCS",
  label = c(
    "Office visit, established patient (level 3)",
    "Office visit, established patient (level 4)",
    "Office visit, established patient (level 5)",
    "ED visit (level 1)",
    "ED visit (level 2)",
    "ED visit (level 3)",
    "ED visit (level 4)",
    "ED visit (level 5)",
    "Brain MRI w/ & w/o contrast",
    "CT abdomen/pelvis w/ contrast",
    "Total knee arthroplasty",
    "Knee arthroscopy/meniscectomy",
    "Upper GI endoscopy w/ biopsy",
    "Electrocardiogram (ECG)",
    "Echocardiography, transthoracic",
    "Cardiac catheterization",
    "Venipuncture (blood draw)",
    "Comprehensive metabolic panel",
    "Complete blood count (CBC)",
    "Chest X-ray, 2 views",
    "MRI lumbar spine w/o contrast",
    "Screening colonoscopy (high risk)",
    "Screening colonoscopy (non-high risk)"
  )
)

# Combined code list --------------------------------------------------------

target_codes <- bind_rows(target_drgs, target_cpts)

message("Code lists loaded: ", nrow(target_drgs), " DRGs, ",
        nrow(target_cpts), " CPTs -> ", nrow(target_codes), " total")
