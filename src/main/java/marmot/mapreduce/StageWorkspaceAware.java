package marmot.mapreduce;

import org.apache.hadoop.fs.Path;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface StageWorkspaceAware {
	public void setStageWorkspace(Path workspaceDir);
	
	public static void set(Object obj, Path workspace) {
		if ( obj instanceof StageWorkspaceAware ) {
			((StageWorkspaceAware)obj).setStageWorkspace(workspace);
		}
	}
}