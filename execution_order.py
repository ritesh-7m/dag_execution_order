from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel, Field
from typing import List, Dict, Optional, Any, Set
from sqlalchemy import create_engine, Column, String, Integer, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship, Session
import networkx as nx

# Database setup
SQLALCHEMY_DATABASE_URL = "sqlite:///./workflow_api.db"
engine = create_engine(SQLALCHEMY_DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Database models
class WorkflowDB(Base):
    __tablename__ = "workflows"
    
    id = Column(Integer, primary_key=True, index=True)
    workflow_str_id = Column(String, unique=True, index=True)
    name = Column(String)
    steps = relationship("StepDB", back_populates="workflow", cascade="all, delete-orphan")


class StepDB(Base):
    __tablename__ = "steps"
    
    id = Column(Integer, primary_key=True, index=True)
    step_str_id = Column(String, index=True)
    description = Column(String)
    workflow_id = Column(Integer, ForeignKey("workflows.id"))
    workflow = relationship("WorkflowDB", back_populates="steps")
    
    # Define unique constraint on (workflow_id, step_str_id)
    __table_args__ = (
        # SQLAlchemy syntax for unique constraint
        # UniqueConstraint('workflow_id', 'step_str_id', name='uix_workflow_step'),
    )


class DependencyDB(Base):
    __tablename__ = "dependencies"
    
    id = Column(Integer, primary_key=True, index=True)
    workflow_id = Column(Integer, ForeignKey("workflows.id"))
    step_str_id = Column(String, index=True)
    prerequisite_step_str_id = Column(String, index=True)


# Create all tables
Base.metadata.create_all(bind=engine)

# Pydantic models for request/response
class WorkflowCreate(BaseModel):
    workflow_str_id: str
    name: str


class WorkflowResponse(BaseModel):
    internal_db_id: int
    workflow_str_id: str
    status: str = "created"


class StepCreate(BaseModel):
    step_str_id: str
    description: str


class StepResponse(BaseModel):
    internal_db_id: int
    step_str_id: str
    status: str = "step_added"


class DependencyCreate(BaseModel):
    step_str_id: str
    prerequisite_step_str_id: str


class DependencyResponse(BaseModel):
    status: str = "dependency_added"


class WorkflowDetails(BaseModel):
    workflow_str_id: str
    name: str
    steps: List[Dict[str, Any]]


class CycleDetectedError(BaseModel):
    error: str = "cycle_detected"


class ExecutionOrder(BaseModel):
    order: List[str]


# Dependency injection
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


app = FastAPI()

# Helper functions
def check_workflow_exists(db: Session, workflow_str_id: str):
    workflow = db.query(WorkflowDB).filter(WorkflowDB.workflow_str_id == workflow_str_id).first()
    if not workflow:
        raise HTTPException(status_code=404, detail=f"Workflow {workflow_str_id} not found")
    return workflow


def check_step_exists(db: Session, workflow_id: int, step_str_id: str):
    step = db.query(StepDB).filter(
        StepDB.workflow_id == workflow_id,
        StepDB.step_str_id == step_str_id
    ).first()
    
    if not step:
        raise HTTPException(
            status_code=404, 
            detail=f"Step {step_str_id} not found in workflow"
        )
    return step


def validate_no_self_dependency(step_str_id: str, prerequisite_step_str_id: str):
    if step_str_id == prerequisite_step_str_id:
        raise HTTPException(
            status_code=400,
            detail="Self-dependency not allowed: step cannot depend on itself"
        )


# Workflow API endpoints
@app.post("/workflows", response_model=WorkflowResponse)
def create_workflow(workflow: WorkflowCreate, db: Session = Depends(get_db)):
    db_workflow = db.query(WorkflowDB).filter(WorkflowDB.workflow_str_id == workflow.workflow_str_id).first()
    if db_workflow:
        raise HTTPException(status_code=400, detail="Workflow ID already exists")
    
    db_workflow = WorkflowDB(workflow_str_id=workflow.workflow_str_id, name=workflow.name)
    db.add(db_workflow)
    db.commit()
    db.refresh(db_workflow)
    
    return {"internal_db_id": db_workflow.id, "workflow_str_id": db_workflow.workflow_str_id}


@app.post("/workflows/{workflow_str_id}/steps", response_model=StepResponse)
def create_step(workflow_str_id: str, step: StepCreate, db: Session = Depends(get_db)):
    workflow = check_workflow_exists(db, workflow_str_id)
    
    # Check if step already exists in this workflow
    existing_step = db.query(StepDB).filter(
        StepDB.workflow_id == workflow.id,
        StepDB.step_str_id == step.step_str_id
    ).first()
    
    if existing_step:
        raise HTTPException(status_code=400, detail=f"Step {step.step_str_id} already exists in workflow")
    
    db_step = StepDB(
        step_str_id=step.step_str_id,
        description=step.description,
        workflow_id=workflow.id
    )
    
    db.add(db_step)
    db.commit()
    db.refresh(db_step)
    
    return {"internal_db_id": db_step.id, "step_str_id": db_step.step_str_id}


@app.post("/workflows/{workflow_str_id}/dependencies", response_model=DependencyResponse)
def create_dependency(workflow_str_id: str, dependency: DependencyCreate, db: Session = Depends(get_db)):
    workflow = check_workflow_exists(db, workflow_str_id)
    
    # Validate no self-dependency
    validate_no_self_dependency(dependency.step_str_id, dependency.prerequisite_step_str_id)
    
    # Check if both steps exist
    check_step_exists(db, workflow.id, dependency.step_str_id)
    check_step_exists(db, workflow.id, dependency.prerequisite_step_str_id)
    
    # Check if dependency already exists
    existing_dependency = db.query(DependencyDB).filter(
        DependencyDB.workflow_id == workflow.id,
        DependencyDB.step_str_id == dependency.step_str_id,
        DependencyDB.prerequisite_step_str_id == dependency.prerequisite_step_str_id
    ).first()
    
    if existing_dependency:
        raise HTTPException(status_code=400, detail="Dependency already exists")
    
    # Create new dependency
    db_dependency = DependencyDB(
        workflow_id=workflow.id,
        step_str_id=dependency.step_str_id,
        prerequisite_step_str_id=dependency.prerequisite_step_str_id
    )
    
    db.add(db_dependency)
    db.commit()
    
    return {"status": "dependency_added"}


@app.get("/workflows/{workflow_str_id}/details", response_model=WorkflowDetails)
def get_workflow_details(workflow_str_id: str, db: Session = Depends(get_db)):
    workflow = check_workflow_exists(db, workflow_str_id)
    
    steps = db.query(StepDB).filter(StepDB.workflow_id == workflow.id).all()
    
    steps_with_prerequisites = []
    for step in steps:
        # Find all prerequisites for this step
        prerequisites = db.query(DependencyDB).filter(
            DependencyDB.workflow_id == workflow.id,
            DependencyDB.step_str_id == step.step_str_id
        ).all()
        
        prerequisite_list = [dep.prerequisite_step_str_id for dep in prerequisites]
        
        steps_with_prerequisites.append({
            "step_str_id": step.step_str_id,
            "description": step.description,
            "prerequisites": prerequisite_list
        })
    
    return {
        "workflow_str_id": workflow.workflow_str_id,
        "name": workflow.name,
        "steps": steps_with_prerequisites
    }


@app.get("/workflows/{workflow_str_id}/execution-order", response_model=ExecutionOrder)
def get_execution_order(workflow_str_id: str, db: Session = Depends(get_db)):
    """
    Implement topological sort (Kahn's algorithm) to determine execution order.
    Returns an error if a cycle is detected.
    """
    workflow = check_workflow_exists(db, workflow_str_id)
    
    # Get all steps in the workflow
    steps = db.query(StepDB).filter(StepDB.workflow_id == workflow.id).all()
    step_ids = {step.step_str_id for step in steps}
    
    # Create a directed graph for dependency relationships
    G = nx.DiGraph()
    
    # Add all steps as nodes
    for step_id in step_ids:
        G.add_node(step_id)
    
    # Get all dependencies
    dependencies = db.query(DependencyDB).filter(
        DependencyDB.workflow_id == workflow.id
    ).all()
    
    # Add dependencies as edges (prerequisite -> step)
    for dep in dependencies:
        G.add_edge(dep.prerequisite_step_str_id, dep.step_str_id)
    
    # Check for cycles
    try:
        # Perform topological sort
        execution_order = list(nx.topological_sort(G))
        return {"order": execution_order}
    except nx.NetworkXUnfeasible:
        # Cycle detected
        return {"error": "cycle_detected"}
