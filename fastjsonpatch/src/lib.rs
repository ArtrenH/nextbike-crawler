use pyo3::prelude::*;
use pyo3::exceptions::PyValueError;
use json_patch::{patch, diff};
use serde_json::{Value, from_str};

/// Apply a JSON patch to a document.
#[pyfunction]
fn apply_patch(doc_str: &str, patch_str: &str) -> PyResult<String> {
    let mut doc: Value = from_str(doc_str).map_err(|e| PyValueError::new_err(e.to_string()))?;
    let patch_val: Value = from_str(patch_str).map_err(|e| PyValueError::new_err(e.to_string()))?;

    let patch_obj: json_patch::Patch = serde_json::from_value(patch_val)
        .map_err(|e| PyValueError::new_err(e.to_string()))?;

    patch(&mut doc, &patch_obj)
        .map_err(|e| PyValueError::new_err(e.to_string()))?;

    Ok(doc.to_string())
}

/// Compute a JSON patch (diff) between two documents.
#[pyfunction]
fn compute_patch(from_json: &str, to_json: &str) -> PyResult<String> {
    let from_doc: Value = serde_json::from_str(from_json)
        .map_err(|e| PyValueError::new_err(e.to_string()))?;
    let to_doc: Value = serde_json::from_str(to_json)
        .map_err(|e| PyValueError::new_err(e.to_string()))?;

    let patch_val = diff(&from_doc, &to_doc);
    Ok(serde_json::to_string(&patch_val).unwrap())
}


/// Python module definition
#[pymodule]
fn fastjsonpatch(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(apply_patch, m)?)?;
    m.add_function(wrap_pyfunction!(compute_patch, m)?)?;
    Ok(())
}
