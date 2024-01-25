package marmot.dataset;

import marmot.proto.CreateSpatialIndexInfoProto;
import marmot.support.PBSerializable;
import utils.Utilities;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class CreateSpatialIndexInfo implements PBSerializable<CreateSpatialIndexInfoProto> {
	private String m_dsId;
	private GeometryColumnInfo m_geomCol;
	private String m_hdfsPath;
	
	public CreateSpatialIndexInfo(String dsId, GeometryColumnInfo geomCol) {
		Utilities.checkNotNullArgument(dsId, "dataset is null");
		Utilities.checkNotNullArgument(geomCol, "geomCol is null");
		
		m_dsId = dsId;
		m_geomCol = geomCol;
	}
	
	/**
	 * 공간 인덱스가 생성된 대상 데이터 세트 식별자를 반환한다.
	 * 
	 * @return	데이터 세트 식별자
	 */
	public String getDataSetId() {
		return m_dsId;
	}
	
	/**
	 * 공간 인덱스 대상 공간 컬럼 정보를 반환한다.
	 * 
	 * @return	공간 컬럼 정보
	 */
	public GeometryColumnInfo getGeometryColumnInfo() {
		return m_geomCol;
	}
	
	public String getHdfsFilePath() {
		return m_hdfsPath;
	}
	
	public void setHdfsFilePath(String path) {
		m_hdfsPath = path;
	}

	public static CreateSpatialIndexInfo fromProto(CreateSpatialIndexInfoProto proto) {
		GeometryColumnInfo geomCol = GeometryColumnInfo.fromProto(proto.getGeometryColumn());
		CreateSpatialIndexInfo info = new CreateSpatialIndexInfo(proto.getDataset(), geomCol);
		info.setHdfsFilePath(proto.getHdfsPath());
		
		return info;
	}

	@Override
	public CreateSpatialIndexInfoProto toProto() {
		return CreateSpatialIndexInfoProto.newBuilder()
									.setDataset(m_dsId)
									.setGeometryColumn(m_geomCol.toProto())
									.setHdfsPath(m_hdfsPath)
									.build();
	}
}
