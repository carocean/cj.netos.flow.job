package cj.netos.flow.job.entities;

public enum GeoCategoryMoveMode {
    /**
     * 固定位置感知器
     */
    unmoveable,
    /**
     * 可自行移动感知器
     */
    moveableSelf,
    /**
     * 需要依赖于感知器创建者的设备感知器定位的
     */
    moveableDependon,
}
