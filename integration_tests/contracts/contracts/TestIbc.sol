// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

contract TestIbcBase {
    struct Params {
        string portId;
        string channelId;
        string srcDenom;
        string dstDenom;
        uint256 ratio;
        uint256 timeout;
    }
}

contract TestIbc is TestIbcBase {
    address constant ibcContract = 0x0000000000000000000000000000000000000065;
    uint256 lastAckSeq;

    function nativeTransfer(Params memory params, address sender, string memory receiver, uint256 amount) public returns (uint256) {
        (bool result, bytes memory data) = ibcContract.call(abi.encodeWithSignature(
            "transfer(string,string,string,string,uint256,uint256,address,string,uint256)",
            params.portId, params.channelId, params.srcDenom, params.dstDenom, params.ratio, params.timeout, sender, receiver, amount
        ));
        require(result, "native call");
        lastAckSeq = abi.decode(data, (uint256));
        return lastAckSeq;
    }
    function nativeHasCommit(string memory portId, string memory channelId, uint256 seq) public returns (bool) {
       (bool result, bytes memory data) = ibcContract.call(abi.encodeWithSignature(
            "hasCommit(string,string,uint256)", portId, channelId, seq
        ));
        require(result, "native call");
        return abi.decode(data, (bool));
    }
    function nativeQueryNextSeq(string memory portId, string memory channelId) public returns (uint256) {
       (bool result, bytes memory data) = ibcContract.call(abi.encodeWithSignature(
            "queryNextSeq(string,string)", portId, channelId
        ));
        require(result, "native call");
        return abi.decode(data, (uint256));
    }
    function getLastAckSeq() public view returns (uint256) {
        return lastAckSeq;
    }
     function nativeTransferRevert(Params memory params, address sender, string memory receiver, uint256 amount) public {
        nativeTransfer(params, sender, receiver, amount);
        revert("test");
    }
}